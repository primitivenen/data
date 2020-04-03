import pandas as pd
from pyhive import hive
import happybase
import logging
import argparse
import numpy as np
import sys
sys.path.insert(0, '/home/prod/git/common/config_utils')
sys.path.insert(0, '/home/prod/git/common/logging')
from load_user_config import load_config_from_file
from user_logging import assign_log_handler, close_log_handler


def cal_new_score(old_score, new_data, factor=0.9, cf_name='cf'):
    new_score = {}
    # use new_data as the new_score
    if not old_score:
        for k in new_data.keys():
            key = (cf_name + ':' + k).encode('utf-8')
            if k not in ['vin', 'day', 'od']:
                new_data[k] = 0 if not new_data[k] else new_data[k]
                new_score[key] = '{0:.2f}'.format(float(new_data[k]))\
                                          .encode('utf-8')
            else:
                new_score[key] = str(new_data[k]).encode('utf-8')
        return (0, 'New score adopted successfully', new_score)

    # if old_score exists, update them using exponential moving average
    if str(old_score[cf_name + ':day']) >= str(new_data['day']):
        msg = ('Score skipped to update, because old_score is {}, '
               'and new_data is {}')\
            .format(old_score[cf_name + ':day'], new_data['day'])
        return (1, msg, None)
    for key in old_score.keys():
        k = key.split(':')[1]  # get rid of column family name
        if k in ['vin', 'day']:
            new_score[key] = str(new_data[k]).encode('utf-8')
        elif k in ['od']:
            new_data[k] = 0 if not new_data[k] else new_data[k]
            new_score[key] = str(np.nanmax([float(old_score[key]),
                    float(new_data[k])])).encode('utf-8')
        else:
            new_data[k] = 0 if not new_data[k] else new_data[k]
            val = np.nansum([factor * float(old_score[key]),
                             (1 - factor) * float(new_data[k])])
            new_score[key] = '{0:.2f}'.format(val).encode('utf-8')
    return (0, 'New score updated successfully', new_score)


def main(args, cfgs):
    hive_con = hive.Connection(host=cfgs.HIVE_HOST)
    hbase_con = happybase.Connection(host=cfgs.HBASE_HOST)
    table = hbase_con.table(cfgs.HBASE_TABLE)

    curr_date = pd.to_datetime(args.day, format='%Y%m%d').strftime('%Y-%m-%d')
    hql = """
        SELECT {}
        FROM {}
        WHERE day = '{}'
    """.format(', '.join(cfgs.TABLE_COLS),
               cfgs.HIVE_TABLE,
               curr_date)

    logger.info('Loading data from Hive')
    data = pd.read_sql(hql, hive_con)
    if data.empty:
        logger.info('No data found in Hive for {}, exiting'.format(curr_date))
        return
    vins = data['vin'].unique()

    logger.info('Updating scores')
    adopts, updates, skips = 0, 0, 0
    for i, vin in enumerate(vins):
        logging.debug('Processing {}'.format(vin))
        old_score = table.row(vin)
        new_data = data[data['vin'] == vin].iloc[0].to_dict()
        status_code, msg, res = cal_new_score(old_score, new_data,
                                              factor=cfgs.EMA_FACTOR,
                                              cf_name=cfgs.HBASE_TABLE_CF)
        logger.debug('{}: {}'.format(vin, msg))
        if 'skip' in msg:
            skips += 1
        elif 'adopt' in msg:
            adopts += 1
        else:
            updates += 1

        if status_code == 0:
            table.put(vin, res, timestamp=int(args.day))

        if i % cfgs.LOGGING_INTERVAL == 0:
            logger.info('{}/{} vehicles processed'.format(i, len(vins)))

    logger.info('vehicle total {}: {} adopts, {} updates, {} skips'
        .format(len(vins), adopts, updates, skips))
    logger.info('done')

if __name__ == '__main__':
    # parse argument from command line
    parser = argparse.ArgumentParser(description='UBI - export stats to hbase.')
    parser.add_argument('-d', '--day', nargs='?', required=True,
        help='specify day, in format of yyyymmdd')
    parser.add_argument('-c', '--config-file', nargs='?', dest='config_file',
        default='export_stats_hbase.cfg',
        help='specify config file, default template is export_stats_hbase.cfg')
    parser.add_argument('-g', '--logfile', nargs='?',
        dest='user_log_file', help='specify log file')
    args = parser.parse_args()

    root_logger = logging.getLogger('')
    logger = logging.getLogger('ubi export stats to hbase')
    assign_log_handler(root_logger=root_logger, log_file=args.user_log_file)

    logger.info('=======')
    logger.info("Loading config")
    cfg_msg, cfgs = load_config_from_file(args.config_file)
    assert cfg_msg == '', 'Fail to load {}, error msg {}'.format(args.config_file, cfg_msg)

    main(args, cfgs)
    logger.info('=======')
    close_log_handler(root_logger)

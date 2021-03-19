
```
"""
@date 2021/3/10
@author: Linda.Chen
"""
import happybase


class HbaseConnector:
    """
        This class provides all Hbase related functions.
    """

    def __init__(self, hbase_port: int = 9090, hbase_host: str = 'ip-10-0-15-199.ec2.internal'):
        self.host = hbase_host
        self.port = hbase_port
        self.conn = happybase.Connection(host=self.host, port=self.port)

    def get_table(self, table_name: str) -> happybase.Table:
        try:
            return self.conn.table(table_name)
        except:
            WriteHbaseException('Table name is not exist in Hbase')

    @staticmethod
    def encode_utf8(cell_val: str) -> str:
        """
            Encode string to byte.
            :param cell_val: string the value that need to change to byte.
            :return: byte format value
        """
        return cell_val.encode('utf-8')

```

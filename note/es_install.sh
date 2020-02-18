#!/bin/sh

# download and install the public signing key
rpm --import https://artifacts.elastic.co/GPG-KEY-elasticsearch

# create the following file
touch /etc/yum.repos.d/elasticsearch.repo

# add to the above file
sh -c 'echo "[elasticsearch-6.x]" >> /etc/yum.repos.d/elasticsearch.repo'
sh -c 'echo "name=Elasticsearch repository for 6.x packages" >> /etc/yum.repos.d/elasticsearch.repo'
sh -c 'echo "baseurl=https://artifacts.elastic.co/packages/6.x/yum" >> /etc/yum.repos.d/elasticsearch.repo'
sh -c 'echo "gpgcheck=1" >> /etc/yum.repos.d/elasticsearch.repo'
sh -c 'echo "gpgkey=https://artifacts.elastic.co/GPG-KEY-elasticsearch" >> /etc/yum.repos.d/elasticsearch.repo'
sh -c 'echo "enabled=1" >> /etc/yum.repos.d/elasticsearch.repo'
sh -c 'echo "autorefresh=1" >> /etc/yum.repos.d/elasticsearch.repo'
sh -c 'echo "type=rpm-md" >> /etc/yum.repos.d/elasticsearch.repo'

# install elasticsearch
yum -y install elasticsearch

# configure elasticsearch to start automatically when the system boots up
/bin/systemctl daemon-reload
/bin/systemctl enable elasticsearch.service

# change the cluster name
sed -i 's/#cluster.name: my-application/cluster.name: es-prod/' /etc/elasticsearch/elasticsearch.yml

# change the node name
sed -i 's/#node.name: node-1/node.name: master-1/' /etc/elasticsearch/elasticsearch.yml

# bind to a non-loopback address
sed -i 's/#network.host: 192.168.0.1/network.host: ["namenode", _local_]/' /etc/elasticsearch/elasticsearch.yml

# provide the list of other nodes in the cluster
#sed -i 's/#discovery.zen.ping.unicast.hosts: ["host1", "host2"]/discovery.zen.ping.unicast.hosts: ["datanode1", "datanode2", "datanode3", "datanode4", "datanode5", "datanode6", "datanode7", "datanode8"]/' /etc/elasticsearch/elasticsearch.yml

# set the minimum number of master-eligible nodes that must be visible in order to form a cluster
sed -i 's/#discovery.zen.minimum_master_nodes:/discovery.zen.minimum_master_nodes: 2/' /etc/elasticsearch/elasticsearch.yml

# set the heap size
sed -i 's/-Xms1g/-Xms8g/' /etc/elasticsearch/jvm.options
sed -i 's/-Xmx1g/-Xmx8g/' /etc/elasticsearch/jvm.options

# change the swappiness
sed -i '$ a\vm.swappiness = 10' /etc/sysctl.conf
sysctl -p

# assign the node to be a dedicated master or data node
sed -i '$ a\node.master: true' /etc/elasticsearch/elasticsearch.yml
sed -i '$ a\node.data: false' /etc/elasticsearch/elasticsearch.yml
sed -i '$ a\node.ml: false' /etc/elasticsearch/elasticsearch.yml

# turn off X-Pack security if you want to use ROR
sed -i '$ a\xpack.security.enabled: false' /etc/elasticsearch/elasticsearch.yml






# disable swapping
#sed -i 's/#bootstrap.memory_lock: true/bootstrap.memory_lock: true/' /etc/elasticsearch/elasticsearch.yml

# grant permission to lock memory
#sed -i '$ a\# Grant permission to lock memory' /usr/lib/systemd/system/elasticsearch.service
#sed -i '$ a\LimitMEMLOCK=infinity' /usr/lib/systemd/system/elasticsearch.service
#systemctl daemon-reload
#sed -i 's/#MAX_LOCKED_MEMORY=unlimited/MAX_LOCKED_MEMORY=unlimited/' /etc/sysconfig/elasticsearch

cd ~/spark-2.1.1-bin-hadoop2.7/sbin
./stop-slave.sh
./stop-master.sh

cd ~/kafka_2.11-0.10.2.0

pkill Elasticsearch -f
pkill kibana -f

./bin/kafka-server-stop.sh

sleep 3

./bin/zookeeper-server-stop.sh




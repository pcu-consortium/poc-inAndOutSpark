path="/home/thest"
spark="spark-2.1.1-bin-hadoop2.7"
kafka="kafka_2.11-0.10.2.0"
elastic="elastic/elasticsearch-5.2.2"
kibana="elastic/kibana-5.2.2-linux-x86_64"

# Démarrage zookeeper & spark master
$path/$spark/sbin/start-master.sh &
$path/$kafka/bin/zookeeper-server-start.sh $path/$kafka/config/zookeeper.properties &
$path/$elastic/bin/elasticsearch&

sleep 3
#Démarrage elastic


$path/$kibana/bin/kibana &

#Démarrage 
$path/$spark/sbin/start-slave.sh spark://P-ASN-Safeword-thest.dhcp.idf.intranet:7077 & 

$path/$kafka/bin/kafka-server-start.sh $path/$kafka/config/server.properties &

sleep 5
#Démarrage kafka

$path/$kafka/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic topic1 & 
echo {"test3":"test3.1","test4":"test4.1", "test5":"test5.1", "test6":"test6.1"} > $path/$kafka/bin/kafka-console-producer.sh --broker-list localhost:9092 --topic topic1




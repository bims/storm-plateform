package kafka;

import kafka.admin.AdminUtils;
import kafka.utils.ZKStringSerializer$;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkMarshallingError;
import org.I0Itec.zkclient.serialize.ZkSerializer;
import otherClass.MyConstants;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Created by sy306571 on 29/11/15.
 */
public class MyKafkaCluster {
    public static void main(String[] args){

        EmbeddedZookeeper embeddedZookeeper = new EmbeddedZookeeper(1984);
        List<Integer> kafkaPorts = new ArrayList<Integer>();
        // -1 for any available port
        kafkaPorts.add(9092);
        EmbeddedKafkaCluster embeddedKafkaCluster = new EmbeddedKafkaCluster(embeddedZookeeper.getConnection(), new Properties(), kafkaPorts);
        try {
            embeddedZookeeper.startup();
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("### Embedded Zookeeper connection: " + embeddedZookeeper.getConnection());
        embeddedKafkaCluster.startup();
        System.out.println("### Embedded Kafka cluster broker list: " + embeddedKafkaCluster.getBrokerList());

        //embeddedKafkaCluster.shutdown();
        //embeddedZookeeper.shutdown();
        // Create a ZooKeeper client
        int sessionTimeoutMs = 10000;
        int connectionTimeoutMs = 10000;
        ZkClient zkClient = new ZkClient("localhost:1984", sessionTimeoutMs, connectionTimeoutMs,ZKStringSerializer$.MODULE$);

        // Create a topic named "gps" with N partition and a replication factor of 1
        String topicName = MyConstants.TOPIC_NAME;
        int numPartitions = MyConstants.NUM_PARTITIONS;
        int replicationFactor = 1;
        Properties topicConfig = new Properties();
        AdminUtils.createTopic(zkClient, topicName, numPartitions, replicationFactor, topicConfig);
    }
}

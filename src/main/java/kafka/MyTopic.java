package kafka;

import kafka.admin.AdminUtils;
import kafka.utils.ZKStringSerializer$;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkMarshallingError;
import org.I0Itec.zkclient.serialize.ZkSerializer;

import java.util.Properties;

/**
 * Created by sy306571 on 29/11/15.
 */
public class MyTopic {
    public static void main(String[] args){
        // Create a ZooKeeper client
        int sessionTimeoutMs = 10000;
        int connectionTimeoutMs = 10000;
        ZkClient zkClient = new ZkClient("localhost:2181", sessionTimeoutMs, connectionTimeoutMs,ZKStringSerializer$.MODULE$);
        //Le parametre avec les dollars à revoir

        // Create a topic named "gps" with 1 partition and a replication factor of 1
        String topicName = "gps";
        int numPartitions = 4;
        int replicationFactor = 1;
        Properties topicConfig = new Properties();
        AdminUtils.createTopic(zkClient, topicName, numPartitions, replicationFactor, topicConfig);
    }
}

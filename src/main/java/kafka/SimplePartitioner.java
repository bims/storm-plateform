package kafka;

import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;

/**
 * Created by sy306571 on 01/12/15.
 */
public class SimplePartitioner implements Partitioner {
    int currentNum = -1;
    public SimplePartitioner(VerifiableProperties props) {

    }

    public int partition(Object key, int a_numPartitions) {
        //currentNum = (currentNum+1)%a_numPartitions;
        //return currentNum;
        return 1;
    }
}

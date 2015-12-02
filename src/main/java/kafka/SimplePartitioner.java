package kafka;

import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;

import java.util.LinkedList;
import java.util.List;
import java.util.Random;

/**
 * Created by sy306571 on 01/12/15.
 */
public class SimplePartitioner implements Partitioner {
    int currentNum = -1;
    public SimplePartitioner(VerifiableProperties props) {

    }

    public int partition(Object key, int a_numPartitions) {
        if(currentNum == a_numPartitions)
            currentNum = -1;
        return currentNum + 1;
    }
}

package otherClass;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * Created by Gawish on 11/30/2015.
 */

public class HBaseTableCreator {

    HTable hTable;

    public HBaseTableCreator(String tablename) throws IOException {
        // Instantiating Configuration class
        Configuration config = HBaseConfiguration.create();

        // Instantiating HTable class
        hTable = new HTable(config, tablename);
    }

    public void addItem(Long id, Double lat, Double lon, String name, String addStreet) throws IOException {
        // Instantiating Put class
        // accepts a row name.
        Put p = new Put(Bytes.toBytes("restaurant" + id));

        // adding values using add() method
        // accepts column family name, qualifier/row name ,value
        p.add(Bytes.toBytes("geo"),
                Bytes.toBytes("lat"),
                Bytes.toBytes(lat));

        p.add(Bytes.toBytes("geo"),
                Bytes.toBytes("lon"),
                Bytes.toBytes(lon));

        p.add(Bytes.toBytes("info"),
                Bytes.toBytes("name"),
                Bytes.toBytes(name));

        p.add(Bytes.toBytes("info"),
                Bytes.toBytes("addr"),
                Bytes.toBytes(addStreet));

        // Saving the put Instance to the HTable.
        hTable.put(p);
        System.out.println("data inserted: Id = " + id + ", Lat = " + lat + ", Long = " + lon + ", Name = " + name);
    }

    public void closeConnection() throws IOException {
        // closing HTable
        hTable.close();
    }

}

package otherClass;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

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

        if(addStreet != null) {
            p.add(Bytes.toBytes("info"),
                    Bytes.toBytes("addr"),
                    Bytes.toBytes(addStreet));
        }
        // Saving the put Instance to the HTable.
        hTable.put(p);
        System.out.println("data inserted: Id = " + id + ", Lat = " + lat + ", Long = " + lon + ", Name = " + name);
    }

    public Restaurant getItem(Long id) throws IOException {
        Restaurant myRestaurant = new Restaurant();

        Get g = new Get(Bytes.toBytes("restaurant" + id));

        // Reading the data
        Result result = hTable.get(g);

        // Reading values from Result class object
        byte [] value = result.getValue(
                Bytes.toBytes("geo"),
                Bytes.toBytes("lat"));

        byte [] value1 = result.getValue(
                Bytes.toBytes("geo"),
                Bytes.toBytes("lon"));

        byte [] value2 = result.getValue(
                Bytes.toBytes("info"),
                Bytes.toBytes("name"));

        byte [] value3 = result.getValue(
                Bytes.toBytes("info"),
                Bytes.toBytes("addr"));

        // Printing the values
        Double lat = Bytes.toDouble(value);
        Double lon = Bytes.toDouble(value1);
        String name = Bytes.toString(value2);
        String addr = null;

        if(value3 != null) {
            addr = Bytes.toString(value3);
        }

        myRestaurant.setId(id);
        myRestaurant.setLat(lat);
        myRestaurant.setLon(lon);
        myRestaurant.getTags().setName(name);
        myRestaurant.getTags().setAddrStreet(addr);

        System.out.println("name: " + name + " addr: " + addr);

        return myRestaurant;
    }

    public void closeConnection() throws IOException {
        // closing HTable
        hTable.close();
    }

}

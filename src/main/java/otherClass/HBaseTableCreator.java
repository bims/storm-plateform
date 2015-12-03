package otherClass;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;

public class HBaseTableCreator {

    HTable hTable;

    public HBaseTableCreator(String tablename, List<String> familyCols) throws IOException {
        Configuration config = HBaseConfiguration.create();

        HBaseAdmin hBaseAdmin = new HBaseAdmin(config);

        HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tablename));

        for(String familyCol : familyCols) {
            tableDescriptor.addFamily(new HColumnDescriptor(familyCol));
        }

        if(!hBaseAdmin.tableExists(tablename)) {
            hBaseAdmin.createTable(tableDescriptor);
        }
    }

    public void addItem(Long id, Double lat, Double lon, String name, String addStreet) throws IOException {
        Put p = new Put(Bytes.toBytes("rest" + id));

        p.add(Bytes.toBytes("geo"),
                Bytes.toBytes("lat"),
                Bytes.toBytes(lat));

        p.add(Bytes.toBytes("geo"),
                Bytes.toBytes("lon"),
                Bytes.toBytes(lon));

        if(name != null) {
            p.add(Bytes.toBytes("info"),
                    Bytes.toBytes("name"),
                    Bytes.toBytes(name));
        }

        if(addStreet != null) {
            p.add(Bytes.toBytes("info"),
                    Bytes.toBytes("addr"),
                    Bytes.toBytes(addStreet));
        }
        hTable.put(p);
        System.out.println("data inserted: Id = " + id +
                                        ", Lat = " + lat +
                                        ", Long = " + lon +
                                        ", Name = " + name);
    }

    public Restaurant getItem(Long id) throws IOException {
        Restaurant myRestaurant = new Restaurant();

        Get g = new Get(Bytes.toBytes("rest" + id));

        Result result = hTable.get(g);

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

        Double lat = Bytes.toDouble(value);
        Double lon = Bytes.toDouble(value1);
        String name = null;
        String addr = null;

        if(value2 != null){
            name = Bytes.toString(value2);
        }

        if(value3 != null) {
            addr = Bytes.toString(value3);
        }

        myRestaurant.setId(id);
        myRestaurant.setLat(lat);
        myRestaurant.setLon(lon);
        myRestaurant.getTags().setName(name);
        myRestaurant.getTags().setAddrStreet(addr);

        System.out.println("name: " + name + " " +
                           "addr: " + addr);

        return myRestaurant;
    }

    public void closeConnection() throws IOException {
        hTable.close();
    }

}

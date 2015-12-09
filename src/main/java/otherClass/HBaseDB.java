package otherClass;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.PageFilter;
import java.io.IOException;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;

public class HBaseDB {

    // Schema Constants - table, column family and column names
    public final static byte[] TABLE_NAME = "restaurants".getBytes();
    public final static byte[] COLUMN_FAMILY_GEO = "geo".getBytes();
    public final static byte[] COLUMN_FAMILY_INFO = "info".getBytes();
    public final static byte[] COL_LON = "lon".getBytes();
    public final static byte[] COL_LAT = "lat".getBytes();
    public final static byte[] COL_NAME = "name".getBytes();
    public final static byte[] COL_ADDR = "addr".getBytes();

    // HBase configuration
    private final Configuration config;

    public HBaseDB(Configuration inConfig) throws IOException {
        config = inConfig;
    }

    public void DropTable() throws IOException {
        try (Connection connection = ConnectionFactory.createConnection(config);
             Admin admin = connection.getAdmin()) {

            HTableDescriptor table = new HTableDescriptor(TableName.valueOf(TABLE_NAME));
            table.addFamily(new HColumnDescriptor(COLUMN_FAMILY_INFO));
            table.addFamily(new HColumnDescriptor(COLUMN_FAMILY_GEO));

            if (admin.tableExists(table.getTableName())) {
                System.out.print("Dropping table. ");
                // a table must be disabled before it can be dropped
                admin.disableTable(table.getTableName());
                admin.deleteTable(table.getTableName());
                System.out.println(" Done.");
            }
        }
    }

    /**
     *    Creates the HBase table
     */
    public void CreateTable() throws IOException {
        try (Connection connection = ConnectionFactory.createConnection(config);
             Admin admin = connection.getAdmin()) {

            HTableDescriptor table = new HTableDescriptor(TableName.valueOf(TABLE_NAME));
            table.addFamily(new HColumnDescriptor(COLUMN_FAMILY_INFO));
            table.addFamily(new HColumnDescriptor(COLUMN_FAMILY_GEO));

            if (!admin.tableExists(table.getTableName())) {
                System.out.print("Creating table. ");
                admin.createTable(table);
                System.out.println(" Done.");
            }
        }
    }

    public Restaurant GetRow(String id) throws IOException {
        Restaurant myRestaurant = new Restaurant();
        try (Connection conn = ConnectionFactory.createConnection(config)){
            // Get the table
            Table table = conn.getTable(TableName.valueOf(TABLE_NAME));
            // Get the result by passing the getter to the table
            Get get = new Get(id.getBytes());
            Result r = table.get(get);
            // return the results
            if ( r.isEmpty() ) return null;
            myRestaurant.setId(new String(r.getRow())); // Gets rowkey from the record for validation
            myRestaurant.setLat(new String(r.getValue(COLUMN_FAMILY_GEO, COL_LAT)));
            myRestaurant.setLon(new String(r.getValue(COLUMN_FAMILY_GEO, COL_LON)));
            myRestaurant.getTags().setName(new String(r.getValue(COLUMN_FAMILY_INFO, COL_NAME)));
            myRestaurant.getTags().setAddrStreet(new String(r.getValue(COLUMN_FAMILY_INFO, COL_ADDR)));

            System.out.println("Gotten the row...");

            return myRestaurant;
        }
    }

    /**
     *    Specifies a range of rows to retrieve based on a starting row key
     *    and retrieves up to limit rows.  Each row is passed to the supplied
     *    DataScanner.
     */
    public void ScanRows(String startID, int limit) throws IOException {
        ResultScanner results = null;
        try (Connection conn = ConnectionFactory.createConnection(config)){
            // Get the table
            Table table = conn.getTable(TableName.valueOf(TABLE_NAME));
            // Create the scan
            Scan scan = new Scan();
            // Tell the server not to cache more than limit rows since we won't need them
            scan.setCaching(limit);
            // Can also set a server side filter
            scan.setFilter(new PageFilter(limit));
            // Get the scan results
            results = table.getScanner(scan);
            // Iterate over the scan results and break at the limit
            int count = 0;
            for (Result r : results) {
                this.ProcessRow(r);
                if ( count++ >= limit ) break;
            }
        }
        finally {
            // ResultScanner must be closed.
            if ( results != null ) results.close();
        }
    }

    public void ProcessRow(Result r) {
        System.out.print(new String(r.getRow()) + ",");
        System.out.print(new String(r.getValue(COLUMN_FAMILY_INFO, COL_NAME)) + ",");
        System.out.print(new String(r.getValue(COLUMN_FAMILY_INFO, COL_ADDR)) + ",");
        System.out.print(new String(r.getValue(COLUMN_FAMILY_GEO, COL_LON)) + ",");
        System.out.print(new String(r.getValue(COLUMN_FAMILY_GEO, COL_LAT)));
    }


    public void addItem(String id, String lat, String lon, String name, String addStreet) throws IOException {
        // Construct a "put" object for insert
        Put p = new Put(id.getBytes());
        p.addColumn(COLUMN_FAMILY_INFO, COL_NAME, name.getBytes());
        if(addStreet!=null)
            p.addColumn(COLUMN_FAMILY_INFO, COL_ADDR, addStreet.getBytes());
        p.addColumn(COLUMN_FAMILY_GEO, COL_LON, lon.getBytes());
        p.addColumn(COLUMN_FAMILY_GEO, COL_LAT, lat.getBytes());

        try (Connection conn = ConnectionFactory.createConnection(config)) {
            Table table = conn.getTable(TableName.valueOf(TABLE_NAME));
            table.put(p);
            table.close();
        }

        System.out.println("data inserted: Id = " + id +
                ", Lat = " + lat +
                ", Long = " + lon +
                ", Name = " + name);
    }
}
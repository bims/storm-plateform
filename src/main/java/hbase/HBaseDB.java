package hbase;

import java.math.BigInteger;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.PageFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import otherClass.Restaurant;
import otherClass.Tags;
import otherClass.RestaurantZValue;

public class HBaseDB {

    // Schema Constants - table, column family and column names
    public final static byte[] TABLE_NAME = "restaurants".getBytes();
    public final static byte[] COLUMN_FAMILY_GEO = "geo".getBytes();
    public final static byte[] COLUMN_FAMILY_INFO = "info".getBytes();
    public final static byte[] COL_LON = "lon".getBytes();
    public final static byte[] COL_LAT = "lat".getBytes();
    public final static byte[] COL_NAME = "name".getBytes();
    public final static byte[] COL_ADDR = "addr".getBytes();
    public final static byte[] COL_ZVALUE = "zvalue".getBytes();
    public final static byte[] COL_PARTITION = "partition".getBytes();


    // HBase configuration
    private final Configuration config;

    public HBaseDB(Configuration inConfig){
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
        Tags myTags = new Tags();
        myRestaurant.setTags(myTags);
        try (Connection conn = ConnectionFactory.createConnection(config)){
            // Get the table
            Table table = conn.getTable(TableName.valueOf(TABLE_NAME));
            // Get the result by passing the getter to the table
            Get get = new Get(id.getBytes());
            Result r = table.get(get);
            // return the results
            if ( r.isEmpty() ) return null;
            myRestaurant.setId(new String(r.getRow())); // Gets rowkey from the record for validation
            String name = new String(r.getValue(COLUMN_FAMILY_INFO, COL_NAME));
            String addr = new String(r.getValue(COLUMN_FAMILY_INFO, COL_ADDR));
            myRestaurant.setLat(new String(r.getValue(COLUMN_FAMILY_GEO, COL_LAT)));
            myRestaurant.setLon(new String(r.getValue(COLUMN_FAMILY_GEO, COL_LON)));

            if(r.getValue(COLUMN_FAMILY_INFO, COL_NAME) != null) {
                name = new String(r.getValue(COLUMN_FAMILY_INFO, COL_NAME));
                myRestaurant.setName(name);
                System.out.println("name="+name);
            }else{
                myRestaurant.setName("Nom inconnu");
            }
            //String addr = new String(r.getValue(COLUMN_FAMILY_INFO, COL_ADDR));

            //myRestaurant.getTags().setAddrStreet("");
            //if(name != null) myRestaurant.getTags().setName(name);
            //if(addr != null) myRestaurant.getTags().setAddrStreet(addr);

            System.out.println("Gotten the row... id="+(new String(r.getRow()))+" x="+(new String(r.getValue(COLUMN_FAMILY_GEO, COL_LAT)))+" y="+(new String(r.getValue(COLUMN_FAMILY_GEO, COL_LON))));
            //if(name != null) System.out.println("name="+name);
            //if(addr != null) System.out.println("address="+addr);
            return myRestaurant;
        } catch (IOException e) {
            e.printStackTrace();
        }

        return null;
    }

    /**
     *    Specifies a range of rows to retrieve based on a starting row key
     *    and retrieves up to limit rows.  Each row is passed to the supplied
     *    DataScanner.
     */
    /**
    public ResultScanner ScanRows(String startID, int limit) throws IOException {
        ResultScanner results = null;
        List<Restaurant> allRestaurants = new ArrayList<>();
        try (Connection conn = ConnectionFactory.createConnection(config)){
            // Get the table
            Table table = conn.getTable(TableName.valueOf(TABLE_NAME));
            // Create the scan
            Scan scan = new Scan();
            // start at a specific rowkey.
            scan.setStartRow(startID.getBytes());
            // Tell the server not to cache more than limit rows since we won't need them
            scan.setCaching(limit);
            // Can also set a server side filter
            scan.setFilter(new PageFilter(limit));
            // Get the scan results
            results = table.getScanner(scan);
            // Iterate over the scan results and break at the limit
            int count = 0;
            for (Result r : results) {
                Restaurant myRestaurant = new Restaurant();
                Tags myTags = new Tags();
                myRestaurant.setTags(myTags);

                if ( r.isEmpty() ) return null;
                myRestaurant.setId(new String(r.getRow())); // Gets rowkey from the record for validation
                String name = new String(r.getValue(COLUMN_FAMILY_INFO, COL_NAME));
                String addr = new String(r.getValue(COLUMN_FAMILY_INFO, COL_ADDR));
                myRestaurant.setLat(new String(r.getValue(COLUMN_FAMILY_GEO, COL_LAT)));
                myRestaurant.setLon(new String(r.getValue(COLUMN_FAMILY_GEO, COL_LON)));
                myRestaurant.getTags().setName("");
                myRestaurant.getTags().setAddrStreet("");
                if(name != null) myRestaurant.getTags().setName(name);
                if(addr != null) myRestaurant.getTags().setAddrStreet(addr);

                allRestaurants.add(myRestaurant);

                if ( count++ >= limit ) break;
            }
        }
        finally {
            // ResultScanner must be closed.
            if ( results != null ) results.close();
            return results;
        }
    }
     **/

    /**
     *    Specifies a range of rows to retrieve based on a starting row key
     *    and retrieves up to limit rows.  Each row is passed to the supplied
     *    DataScanner.
     */
    public List<Restaurant> ScanRows(String startID, int limit) throws IOException {
        ResultScanner results = null;
        List<Restaurant> allRestaurants = new ArrayList<>();
        try (Connection conn = ConnectionFactory.createConnection(config)){
            // Get the table
            Table table = conn.getTable(TableName.valueOf(TABLE_NAME));
            // Create the scan
            Scan scan = new Scan();
            // start at a specific rowkey.
            scan.setStartRow(startID.getBytes());
            // Tell the server not to cache more than limit rows since we won't need them
            scan.setCaching(limit);
            // Can also set a server side filter
            scan.setFilter(new PageFilter(limit));
            // Get the scan results
            results = table.getScanner(scan);
            // Iterate over the scan results and break at the limit
            int count = 0;
            for (Result r : results) {
                Restaurant myRestaurant = new Restaurant();
                //Tags myTags = new Tags();
                //myRestaurant.setTags(myTags);

                if ( r.isEmpty() ) return null;
                myRestaurant.setId(new String(r.getRow())); // Gets rowkey from the record for validation
                //String addr = new String(r.getValue(COLUMN_FAMILY_INFO, COL_ADDR));
                myRestaurant.setLat(new String(r.getValue(COLUMN_FAMILY_GEO, COL_LAT)));
                myRestaurant.setLon(new String(r.getValue(COLUMN_FAMILY_GEO, COL_LON)));
                //myRestaurant.getTags().setAddrStreet("");
                myRestaurant.setName(new String(r.getValue(COLUMN_FAMILY_INFO, COL_NAME)));
                //if(addr != null) myRestaurant.getTags().setAddrStreet(addr);

                allRestaurants.add(myRestaurant);

                if ( count++ >= limit ) break;
            }
        }
        finally {
            // ResultScanner must be closed.
            if ( results != null ) results.close();
        }
        return allRestaurants;
    }

    public List<RestaurantZValue> ScanZRows(String startID, int limit) throws IOException {
        ResultScanner results = null;
        List<RestaurantZValue> allRestaurants = new ArrayList<>();
        try (Connection conn = ConnectionFactory.createConnection(config)){
            // Get the table
            Table table = conn.getTable(TableName.valueOf(TABLE_NAME));
            // Create the scan
            Scan scan = new Scan();
            // start at a specific rowkey.
            scan.setStartRow(startID.getBytes());
            // Tell the server not to cache more than limit rows since we won't need them
            scan.setCaching(limit);
            // Can also set a server side filter
            scan.setFilter(new PageFilter(limit));
            // Get the scan results
            results = table.getScanner(scan);
            // Iterate over the scan results and break at the limit
            int count = 0;
            for (Result r : results) {
                RestaurantZValue myRestaurant = new RestaurantZValue();
                //Tags myTags = new Tags();
                //myRestaurant.setTags(myTags);

                if ( r.isEmpty() ) return null;
                myRestaurant.setId(new String(r.getRow())); // Gets rowkey from the record for validation
                //String addr = new String(r.getValue(COLUMN_FAMILY_INFO, COL_ADDR));
                myRestaurant.setLat(new String(r.getValue(COLUMN_FAMILY_GEO, COL_LAT)));
                myRestaurant.setLon(new String(r.getValue(COLUMN_FAMILY_GEO, COL_LON)));
                //myRestaurant.getTags().setAddrStreet("");
                myRestaurant.setName(new String(r.getValue(COLUMN_FAMILY_INFO, COL_NAME)));
                myRestaurant.setzValue(new BigInteger(new String(r.getValue(COLUMN_FAMILY_GEO, COL_ZVALUE))));
                //if(addr != null) myRestaurant.getTags().setAddrStreet(addr);

                allRestaurants.add(myRestaurant);

                if ( count++ >= limit ) break;
            }
        }
        finally {
            // ResultScanner must be closed.
            if ( results != null ) results.close();
        }
        return allRestaurants;
    }

    public void ProcessRow(Result r) {
        System.out.print(new String(r.getRow()) + ",");
        System.out.print(new String(r.getValue(COLUMN_FAMILY_INFO, COL_NAME)) + ",");
        System.out.print(new String(r.getValue(COLUMN_FAMILY_INFO, COL_ADDR)) + ",");
        System.out.print(new String(r.getValue(COLUMN_FAMILY_GEO, COL_LON)) + ",");
        System.out.print(new String(r.getValue(COLUMN_FAMILY_GEO, COL_LAT)));
    }

    public Restaurant ProcessRowR(Result r) {
        Restaurant myRestaurant = new Restaurant();
        myRestaurant.setLat(new String(r.getValue(COLUMN_FAMILY_GEO, COL_LAT)));
        myRestaurant.setLon(new String(r.getValue(COLUMN_FAMILY_GEO, COL_LON)));

        return myRestaurant;
    }

    public void addItem(Table t, String id, String lat, String lon, String name) throws IOException {
    //public void addItem(String id, String lat, String lon, String name, String addStreet) throws IOException {
        // Construct a "put" object for insert
        Put p = new Put(id.getBytes());

        p.addColumn(COLUMN_FAMILY_INFO, COL_NAME, name.getBytes());
        //if(addStreet!=null)
          //  p.addColumn(COLUMN_FAMILY_INFO, COL_ADDR, addStreet.getBytes());
        p.addColumn(COLUMN_FAMILY_GEO, COL_LON, lon.getBytes());
        p.addColumn(COLUMN_FAMILY_GEO, COL_LAT, lat.getBytes());

        t.put(p);

        System.out.println("data inserted: Id = " + id +
                ", Lat = " + lat +
                ", Long = " + lon +
                ", Name = " + name);
    }

    public void addItemZValue(Table table, String id, String lat, String lon, String name, BigInteger zValue) throws IOException {
        //public void addItem(String id, String lat, String lon, String name, String addStreet) throws IOException {
        // Construct a "put" object for insert
        Put p = new Put(id.getBytes());

        p.addColumn(COLUMN_FAMILY_INFO, COL_NAME, name.getBytes());
        //if(addStreet!=null)
        // p.addColumn(COLUMN_FAMILY_INFO, COL_ADDR, addStreet.getBytes());
        p.addColumn(COLUMN_FAMILY_GEO, COL_LON, lon.getBytes());
        p.addColumn(COLUMN_FAMILY_GEO, COL_LAT, lat.getBytes());

        //Colonne pour zValue
        p.addColumn(COLUMN_FAMILY_GEO, COL_ZVALUE, zValue.toString().getBytes());

        table.put(p);

        System.out.println("data inserted: Id = " + id +
                ", Lat = " + lat +
                ", Long = " + lon +
                ", Name = " + name +
                ", ZValue = " + zValue);
    }

    public static int[] getNbItems(int size, int nbParts){
        int nbTuples[] = new int[nbParts];
        int remaining = size;
        int h=0;
        while(remaining > 0){
            nbTuples[h]++;
            remaining--;
            h++;
            if(h==nbParts)
                h=0;
        }

        return nbTuples;
    }

    public static int[] getStartIds(int nbParts, int nbTuples[]){
        int startId[] = new int[nbParts];
        int sum = 0;
        int h=0;
        for(h=0; h<nbParts; h++){
            startId[h] = sum;
            sum+=nbTuples[h];
        }

        return startId;
    }

        //POUR RECUPERER LES BONS INDICES
    public static int[] getIndiceDB(int size, int nbParts){

        int[] res = new int[nbParts];

        //On récupère les indices réels que l'on souhaite
        for(int i = 0; i<nbParts; i++){

            res[i] = (i*size/nbParts)+1;

        }

        ArrayList<String> indices = new ArrayList<String>();

        //On cree une liste dans l'ordre numérique
        for(int i = 1; i<=size; i++){

            indices.add(""+i);

        }

        //On trie la liste par ordre alphabetique
        Collections.sort(indices);

        //On affecte le nouvel indice
        String newIndice = "";
        for(int i = 0; i<nbParts; i++){


            newIndice = indices.get((i*size/nbParts));

            res[i] = Integer.parseInt(newIndice);

            //System.out.println(newIndice);

        }

        return res;

    }
}

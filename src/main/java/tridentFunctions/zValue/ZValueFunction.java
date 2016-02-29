package tridentFunctions.zValue;

import backtype.storm.tuple.Values;
import convert_coord.Zorder;
import convert_coord.data_preprocessing;
import inputClass.Input;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

import java.math.BigInteger;
import java.util.Random;

public class ZValueFunction extends BaseFunction {

    @Override
    public void execute(TridentTuple tridentTuple, TridentCollector tridentCollector) {
        int scale = 1000;
        Input inp = (Input) tridentTuple.getValue(0);
        double[] query = new double[2];
        query[0] = Double.parseDouble(inp.getX());
        query[1] = Double.parseDouble(inp.getY());
        int[] convertCoord = Zorder.convertCoord(1, 2, scale, new int[2][2], query);
        String zValue = String.valueOf(Zorder.fromStringToInt(Zorder.valueOf(2, convertCoord)));
        BigInteger bigInt = new BigInteger(zValue);

        tridentCollector.emit(new Values(bigInt));


        //int[] coord_resto=;
        /*int dimension = 2;
        int shift = 3;

        int[] coord = new int[dimension];
        Random r = new Random();
        int[] converted_coord;

        String zval = null;

        Input inp = (Input) tridentTuple.getValue(0);
        coord[0] = Double.parseDouble(inp.getX());
        coord[1] = Double.parseDouble(inp.getY());


        // Generate random shift vectors
        converted_coord= data_preprocessing.Convertcoord(dimension, coord);
        System.out.println("coordonnee initiale");
        for (int i = 0; i < dimension; i++)
            System.out.println(coord[i]);

        System.out.println("coordonnee convertie");
        for (int i = 0; i < dimension; i++)
            System.out.println(converted_coord[i]);


        zval = Zorder.valueOf(dimension, converted_coord);

        System.out.println("zval");
        System.out.println(zval);
        // Test case*/
    }
}

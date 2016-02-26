package tridentFunctions.zValue;

import backtype.storm.tuple.Values;
import convert_coord.Zorder;
import convert_coord.data_preprocessing;
import inputClass.Input;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

import java.util.Random;

public class ZValueFunction extends BaseFunction {

    @Override
    public void execute(TridentTuple tridentTuple, TridentCollector tridentCollector) {
        Input inp = (Input) tridentTuple.getValue(0);
        double x = Double.parseDouble(inp.getX());
        double y = Double.parseDouble(inp.getX());
        tridentCollector.emit(new Values((x + y) * ((x + y))));


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
package tridentFunctions.zValue;

import backtype.storm.tuple.Values;
import convert_coord.Zorder;
import inputClass.Input;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

import java.math.BigInteger;

public class ZValueFunction extends BaseFunction {

    @Override
    public void execute(TridentTuple tridentTuple, TridentCollector tridentCollector) {
        int scale = 1000;
        Input inp = (Input) tridentTuple.getValue(0);
        double[] query = new double[2];
        query[0] = Double.parseDouble(inp.getX());
        query[1] = Double.parseDouble(inp.getY());
        int[] convertCoord = Zorder.convertCoord(1, 2, scale, new int[2][2], query);
        String zValue = String.valueOf(Zorder.eraseZeros(Zorder.valueOf(2, convertCoord)));
        BigInteger bigInt = new BigInteger(zValue);

        tridentCollector.emit(new Values(bigInt));
    }
}

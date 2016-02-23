package testTridentFunctions;

import storm.trident.operation.BaseFilter;
import storm.trident.tuple.TridentTuple;

/**
 * Created by sy306571 on 21/02/16.
 */
public class PrintFilter extends BaseFilter {

    public PrintFilter() {

    }
    @Override
    public boolean isKeep(TridentTuple tuple) {
        if(!tuple.getValue(0).toString().equals("{}"))
            System.out.println(tuple.getValue(0));
        return false;
    }
}

package tridentFunctions;

import backtype.storm.tuple.Values;
import inputClass.Input;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

/**
 * Created by sy306571 on 16/01/16.
 */
public class InputNormalizerFunction extends BaseFunction {
    public void execute(TridentTuple tuple, TridentCollector collector) {
        String sentence = new String(tuple.getBinary(0));
        String[] words = sentence.split(" ");
        
        int nbParts = 4;
        
        String x = words[0];
        String y = words[1];
        String part = "";
        
        for(int i = 1; i <= nbParts; i++){
            part = new Integer(i).toString();
            Input obj = new Input(x,y);
            collector.emit(new Values(obj, part));
        }
    }
}

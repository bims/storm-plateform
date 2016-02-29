package inputClass;

import java.math.BigInteger;

public class InputZValue extends Input{
    private BigInteger zValue;


    public InputZValue(String x, String y, BigInteger zValue) {
        super(x, y);
        this.zValue = zValue;
    }

    public InputZValue(Input inp, BigInteger zValue) {
        super(inp.x, inp.y);
        this.zValue = zValue;
    }

    public BigInteger getzValue() {
        return zValue;
    }

    public void setzValue(BigInteger zValue) {
        this.zValue = zValue;
    }
}

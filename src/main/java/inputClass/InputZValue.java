package inputClass;

public class InputZValue extends Input{
    private double zValue;


    public InputZValue(String x, String y, double zValue) {
        super(x, y);
        this.zValue = zValue;
    }

    public InputZValue(Input inp, double zValue) {
        super(inp.x, inp.y);
        this.zValue = zValue;
    }

    public double getzValue() {
        return zValue;
    }

    public void setzValue(double zValue) {
        this.zValue = zValue;
    }
}

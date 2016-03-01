package otherClass;

import java.math.BigInteger;

public class ZLimits{

    private BigInteger borneInf;
    private BigInteger borneSup;

    public ZLimits(BigInteger borneInf, BigInteger borneSup) {
        this.borneInf = borneInf;
        this.borneSup = borneSup;
    }

    public BigInteger getBorneInf() {
        return borneInf;
    }
    public BigInteger getBorneSup() {
        return borneSup;
    }
}

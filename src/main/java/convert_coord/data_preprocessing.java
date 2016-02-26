package convert_coord;

import java.util.Random;

/**
 * Created by asmaafillatre on 23/02/2016.s
 */
public class data_preprocessing {


    public static int[] Convertcoord(int dimension, int[] coord) {

        float[] tmp_coord = new float[dimension];
        int scale = 1000;
        int shift = 3;

        Random r = new Random();


        int[][] shiftvectors = new int[shift][dimension];

        // Generate random shift vectors
        for (int i = 0; i < shift; i++)
            shiftvectors[i] = Zorder.createShift(dimension, r, true);

        // Scale up coordinates and add random shift vector
        int[] converted_coord = new int[dimension];

        for (int i = 0; i < shift; i++) {
            for (int k = 0; k < dimension; k++) {
                tmp_coord[k] = coord[k];
                // To prevent precision loss, we need to ale up
                // the part behinde the decimal point to integer.
                converted_coord[k] = (int) tmp_coord[k]; // Get integer part
                tmp_coord[k] -= converted_coord[k];  // Get fractional part
                converted_coord[k] *= scale;         // Scale integer part
                converted_coord[k] += (tmp_coord[k] * scale);
                if (i != 0)   //for shift 0 we use the original setting
                    converted_coord[k] += shiftvectors[i][k]; // Add shift
            }

        }
        return converted_coord;
    }

    public static void main(String[] args) {
        //int[] coord_resto=;
        int dimension = 2;
        int shift = 3;

        int[] coord = new int[dimension];
        Random r = new Random();
        int[] converted_coord;
        int [] zvalues =new int[dimension];

        String zval = null;

        coord[0] = 2;
        coord[1] = 6;

        // Generate random shift vectors
        converted_coord=Convertcoord(dimension,coord);
        System.out.println("coordonnee initiale");

        for (int i = 0; i < dimension; i++)
            System.out.println(coord[i]);

        //System.out.println("coordonnee convertie");
        //for (int i = 0; i < dimension; i++)
          //  System.out.println(converted_coord[i]);

            zval = Zorder.valueOf(dimension, coord);
            System.out.println("zval of converted coord");


        //n Collections.sort(zvalues, new InputCompareToDBFunction.DistanceComparator());
            System.out.println(zval);
            // Test case
    }

}

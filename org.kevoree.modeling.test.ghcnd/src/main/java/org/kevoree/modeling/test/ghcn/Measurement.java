package org.kevoree.modeling.test.ghcn;

import java.util.Collection;
import java.util.HashMap;

/**
 * Created by gregory.nain on 01/08/2014.
 */
public abstract class Measurement {

    public static class MeasurementType {
        public String key;
        public String name;
        public String description;

        public MeasurementType(String key, String name, String description) {
            this.key = key;
            this.name = name;
            this.description = description;
        }
    }

    public static final MeasurementType BLANK = new MeasurementType("", "N/A", "No measurement information applicable");
    public static final MeasurementType B = new MeasurementType("B", "2 x 12h", "Precipitation total formed from two 12-hour totals");
    public static final MeasurementType D = new MeasurementType("D", "4 x 6h", "Precipitation total formed from four six-hour totals");
    public static final MeasurementType H = new MeasurementType("H", "Highest or Lowest", "Represents highest or lowest hourly temperature");
    public static final MeasurementType K = new MeasurementType("K", "From Knots", "Converted from Knots");
    public static final MeasurementType L = new MeasurementType("L", "Lagged", "Temperature appears to be lagged with respect to reported hour of observation");
    public static final MeasurementType O  = new MeasurementType("O", "From Oktas", "Converted from Oktas");
    public static final MeasurementType P = new MeasurementType("P", "Missing", "Identified as \"missing presumed zero\" in DSI 3200 and 3206");
    public static final MeasurementType T = new MeasurementType("T", "Traces", "trace of precipitation, snowfall, or snow depth");
    public static final MeasurementType W = new MeasurementType("", "From WBAN(16 points)", "Converted from 16-point WBAN code (for wind direction)");

    private static final HashMap<String, MeasurementType> measurementTypes = new HashMap<String, MeasurementType>(10);

    static{
        measurementTypes.put(BLANK.key, BLANK);
        measurementTypes.put(B.key, B);
        measurementTypes.put(D.key, D);
        measurementTypes.put(H.key, H);
        measurementTypes.put(K.key, K);
        measurementTypes.put(L.key, L);
        measurementTypes.put(O.key, O);
        measurementTypes.put(P.key, P);
        measurementTypes.put(T.key, T);
        measurementTypes.put(W.key, W);
    }


    public static Collection<MeasurementType> getMeasurementTypeList() {
        return measurementTypes.values();
    }

    public static MeasurementType parse(String s) {
        return measurementTypes.get(s);
    }

}

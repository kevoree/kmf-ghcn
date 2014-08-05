package org.kevoree.modeling.test.ghcn;

import java.util.Collection;
import java.util.HashMap;

/**
 * Created by gregory.nain on 01/08/2014.
 */
public abstract class Source {

    public static class SourceType {
        public String key;
        public String name;
        public String description;

        public SourceType(String key, String name, String description) {
            this.key = key;
            this.name = name;
            this.description = description;
        }
    }

    public static final SourceType BLANK = new SourceType("", "N/A", "No source (i.e., data value missing)");
    public static final SourceType ZERO = new SourceType("0", "NCDC DSI-3200", "U.S. Cooperative Summary of the Day (NCDC DSI-3200)");
    public static final SourceType SIX = new SourceType("6", "NCDC DSI-3206", "CDMP Cooperative Summary of the Day (NCDC DSI-3206)");
    public static final SourceType SEVEN = new SourceType("7", "NCDC DSI-3207", "U.S. Cooperative Summary of the Day -- Transmitted via WxCoder3 (NCDC DSI-3207)");
    public static final SourceType A = new SourceType("A", "ASOS", "U.S. Automated Surface Observing System (ASOS) real-time data (since January 1, 2006)");
    public static final SourceType B = new SourceType("B", "ASOS", "U.S. ASOS data for October 2000-December 2005 (NCDC DSI-3211)");
    public static final SourceType a = new SourceType("a", "Australian Meteorology", "Australian data from the Australian Bureau of Meteorology");
    public static final SourceType b = new SourceType("b", "Belarus", "Belarus update");
    public static final SourceType E = new SourceType("E", "ECAD", "European Climate Assessment and Dataset (Klein Tank et al., 2002)");
    public static final SourceType F = new SourceType("F", "U.S. Fort data", "U.S. Fort data");
    public static final SourceType G = new SourceType("G", "GCOS", "Official Global Climate Observing System (GCOS) or other government-supplied data");
    public static final SourceType H = new SourceType("H", "HPRC", "High Plains Regional Climate Center real-time data");
    public static final SourceType I = new SourceType("I", "Int. Collection", "International collection (non U.S. data received through personal contacts)");
    public static final SourceType K = new SourceType("K", "Digitalized Paper", "U.S. Cooperative Summary of the Day data digitized from paper observer forms (from 2011 to present)");
    public static final SourceType M = new SourceType("M", "METAR", "Monthly METAR Extract (additional ASOS data)");
    public static final SourceType N = new SourceType("N", "CoCoRaHS", "Community Collaborative Rain, Hail,and Snow (CoCoRaHS)");
    public static final SourceType Q = new SourceType("Q", "African countries", "Data from several African countries");
    public static final SourceType R = new SourceType("R", "NCDC", "NCDC Reference Network Database (Climate Reference Network and Historical Climatology Network-Modernized)");
    public static final SourceType r = new SourceType("r", "Russian Research Institute", "All-Russian Research Institute of Hydrometeorological Information-World Data Center");
    public static final SourceType S = new SourceType("S", "NCDC DSI-9618", "Global Summary of the Day (NCDC DSI-9618)");
    public static final SourceType s = new SourceType("s", "China Meteorological", "China Meteorological Administration/National Meteorological Information Center/ Climatic Data Center (http://cdc.cma.gov.cn)");
    public static final SourceType T = new SourceType("T", "SNOTEL", "SNOwpack TELemtry (SNOTEL) data obtained from the Western Regional Climate Center");
    public static final SourceType U = new SourceType("U", "RAWS", "Remote Automatic Weather Station (RAWS) data obtained from the Western Regional Climate Center");
    public static final SourceType u = new SourceType("u", "Ukraine update", "Ukraine update");
    public static final SourceType W = new SourceType("W", "WBAN/ASOS", "WBAN/ASOS Summary of the Day from NCDC's Integrated Surface Data (ISD)");
    public static final SourceType X = new SourceType("X", "NCDC DSI-3210", "U.S. First-Order Summary of the Day (NCDC DSI-3210)");
    public static final SourceType Z = new SourceType("Z", "Datzilla", "Datzilla official additions or replacements");
    public static final SourceType z = new SourceType("z", "Uzbekistan", "Uzbekistan update");

    private static final HashMap<String, SourceType> sourceTypes = new HashMap<String, SourceType>(10);

    static{
        sourceTypes.put(BLANK.key, BLANK);
        sourceTypes.put(ZERO.key, ZERO);
        sourceTypes.put(SIX.key, SIX);
        sourceTypes.put(SEVEN.key, SEVEN);
        sourceTypes.put(A.key, A);
        sourceTypes.put(B.key, B);
        sourceTypes.put(a.key, a);
        sourceTypes.put(b.key, b);
        sourceTypes.put(E.key, E);
        sourceTypes.put(F.key, F);
        sourceTypes.put(G.key, G);
        sourceTypes.put(H.key, H);
        sourceTypes.put(I.key, I);
        sourceTypes.put(K.key, K);
        sourceTypes.put(M.key, M);
        sourceTypes.put(N.key, N);
        sourceTypes.put(Q.key, Q);
        sourceTypes.put(R.key, R);
        sourceTypes.put(r.key, r);
        sourceTypes.put(S.key, S);
        sourceTypes.put(s.key, s);
        sourceTypes.put(T.key, T);
        sourceTypes.put(U.key, U);
        sourceTypes.put(u.key, u);
        sourceTypes.put(W.key, W);
        sourceTypes.put(X.key, X);
        sourceTypes.put(Z.key, Z);
        sourceTypes.put(z.key, z);
    }


    public static Collection<SourceType> getMeasurementTypeList() {
        return sourceTypes.values();
    }

    public static SourceType parse(String s) {
        return sourceTypes.get(s);
    }


}

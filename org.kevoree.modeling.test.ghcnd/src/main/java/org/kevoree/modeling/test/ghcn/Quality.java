package org.kevoree.modeling.test.ghcn;

import java.util.Collection;
import java.util.HashMap;

/**
 * Created by gregory.nain on 01/08/2014.
 */
public abstract class Quality {

    public static class QualityAttribute {
        public String key;
        public String name;
        public String description;

        public QualityAttribute(String key, String name, String description) {
            this.key = key;
            this.name = name;
            this.description = description;
        }
    }

    public static final QualityAttribute BLANK = new QualityAttribute("", "OK", "Did not fail any quality assurance check");
    public static final QualityAttribute D = new QualityAttribute("D", "Duplicated", "Failed duplicate check");
    public static final QualityAttribute G = new QualityAttribute("G", "Gaped", "Failed gap check");
    public static final QualityAttribute I = new QualityAttribute("I", "Inconsistent", "Failed internal consistency check");
    public static final QualityAttribute K = new QualityAttribute("K", "Failed S/F", "Failed streak/frequent-value check");
    public static final QualityAttribute L = new QualityAttribute("L", "Failed Multiday", "Failed check on length of multiday period");
    public static final QualityAttribute M = new QualityAttribute("M", "Failed Megaconsistency", "Failed megaconsistency check");
    public static final QualityAttribute N = new QualityAttribute("N", "Failed Naught", "Failed naught check");
    public static final QualityAttribute O = new QualityAttribute("O", "Failed Outlier", "Failed climatological outlier check");
    public static final QualityAttribute R = new QualityAttribute("R", "Failed Lagged", "Failed lagged range check");
    public static final QualityAttribute S = new QualityAttribute("S", "Failed Spatial Consistency", "Failed spatial consistency check");
    public static final QualityAttribute T = new QualityAttribute("T", "Failed Temporal Consistency", "Failed temporal consistency check");
    public static final QualityAttribute W = new QualityAttribute("W", "Too Warm", "Temperature too warm for snow");
    public static final QualityAttribute X = new QualityAttribute("X", "Failed Bounds", "Failed bounds check");
    public static final QualityAttribute Z = new QualityAttribute("Z", "Investigation Result", "flagged as a result of an official Datzilla investigation");

    private static final HashMap<String, QualityAttribute> qualityAttributes = new HashMap<String, QualityAttribute>(15);

    static {
        qualityAttributes.put(BLANK.key, BLANK);
        qualityAttributes.put(D.key,D);
        qualityAttributes.put(G.key,G);
        qualityAttributes.put(I.key,I);
        qualityAttributes.put(K.key,K);
        qualityAttributes.put(L.key,L);
        qualityAttributes.put(M.key,M);
        qualityAttributes.put(N.key,N);
        qualityAttributes.put(O.key,O);
        qualityAttributes.put(R.key,R);
        qualityAttributes.put(S.key,S);
        qualityAttributes.put(T.key,T);
        qualityAttributes.put(W.key,W);
        qualityAttributes.put(X.key,X);
        qualityAttributes.put(Z.key,Z);
    }

    public static Collection<QualityAttribute> getAttibutesList() {
        return qualityAttributes.values();
    }

    public static QualityAttribute parse(String s) {
        QualityAttribute tmp = qualityAttributes.get(s);
        if(tmp == null) {
            System.out.println("Not found quality with key:" + s);
        }
        return  tmp;
    }
}
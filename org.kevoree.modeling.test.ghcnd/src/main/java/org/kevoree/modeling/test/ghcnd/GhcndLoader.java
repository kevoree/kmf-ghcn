package org.kevoree.modeling.test.ghcnd;

import kmf.ghcnd.DataSet;
import kmf.ghcnd.GhcndFactory;
import kmf.ghcnd.impl.DefaultGhcndFactory;
import org.kevoree.modeling.api.KMFContainer;
import org.kevoree.modeling.api.time.TimeView;
import org.kevoree.modeling.datastores.leveldb.LevelDbDataStore;

import java.io.*;

/**
 * Created by gregory.nain on 22/07/2014.
 */
public class GhcndLoader {

    private String dbLocation = "GhcndLeveDB";
    private GhcndFactory baseFactory;

    public void initFactory() {

        baseFactory = new DefaultGhcndFactory();
        baseFactory.setDatastore(new LevelDbDataStore(dbLocation));
        TimeView<GhcndFactory> rootTimeView = baseFactory.time("0");
        DataSet root = (DataSet)rootTimeView.lookup("/");
        if(root == null) {
            root = rootTimeView.factory().createDataSet();
            rootTimeView.root(root);
            rootTimeView.commit();
        }
        /*

        DataSet root = (DataSet)baseFactory.lookup("/");
        if(root == null) {
            root = baseFactory.createDataSet();
            baseFactory.root(root);
            //root.setGenerated_KMF_ID("jj");
            baseFactory.commit();
        }
        */

    }


    public void updateCountries() {
        GhcnCountriesManager cm = new GhcnCountriesManager(baseFactory);
        cm.updateCountries();
        System.out.println(cm.toString());
    }

    public void updateUSStates() {
        GhcnUSStatesManager cm = new GhcnUSStatesManager(baseFactory);
        cm.updateStates();
        System.out.println(cm.toString());
    }


    public void updateStations() {
        GhcnStationsManager cm = new GhcnStationsManager(baseFactory);
        cm.updateStations();
        System.out.println(cm.toString());
    }


    public void updateDaily() {
        GhcnDailyManager cm = new GhcnDailyManager(baseFactory);
        cm.updateDailyValues();
    }
}

package org.kevoree.modeling.test.ghcn;

import kmf.ghcn.DataSet;
import kmf.ghcn.factory.DefaultGhcnFactory;
import kmf.ghcn.factory.GhcnFactory;
import org.kevoree.modeling.api.time.TimeView;
import org.kevoree.modeling.datastores.leveldb.LevelDbDataStore;
import org.kevoree.modeling.test.ghcn.utils.Stats;
import org.kevoree.modeling.test.ghcn.utils.ThreadPoolManager;
import org.kevoree.modeling.test.ghcn.utils.UpdateResult;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.TimeZone;
import java.util.concurrent.*;

/**
 * Created by gregory.nain on 22/07/2014.
 */
public class GhcnLoader {

    private String dbLocation = "GhcnLevelDB";
    private GhcnFactory baseFactory;

    public GhcnLoader() {
        initFactory();
    }


    public GhcnLoader(String dbLocation) {
        this.dbLocation=dbLocation;
        initFactory();
    }

    private void initFactory() {

        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMd");
        simpleDateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));


        baseFactory = new DefaultGhcnFactory();
        baseFactory.setDatastore(new LevelDbDataStore(dbLocation));
        TimeView<GhcnFactory> rootTimeView = null;
        try {
            rootTimeView = baseFactory.time(simpleDateFormat.parse("18000101").getTime() + "");
        } catch (ParseException e) {
            e.printStackTrace();
        }
        DataSet root = (DataSet)rootTimeView.lookup("/");
        //DataSet root = (DataSet)baseFactory.lookup("/");
        if(root == null) {
            /*
            root = baseFactory.createDataSet();
            baseFactory.root(root);
            baseFactory.commit();
            */
            root = rootTimeView.factory().createDataSet();
            rootTimeView.root(root);
            rootTimeView.commit();
        }
    }

    public void free(){
        baseFactory.getDatastore().sync();
    }

    public void updateAll() {
        updateCountries();
        updateUSStates();
        updateStations();
        updateDaily();
    }

    public void waitCompletion() {
        while(ThreadPoolManager.waitingExecution.size() > 0) {
            try {
                Future<UpdateResult> f = ThreadPoolManager.waitingExecution.poll();
                UpdateResult result = f.get();
                for(Stats s : result.statistics) {
                    System.out.println(s);
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        }


    }

    public void commitAll() {
        baseFactory.commitAll();
        //baseFactory.commit();
    }

    public void updateCountries() {
        ThreadPoolManager.addTask(new GhcnCountriesManager(baseFactory));
        //(new GhcnCountriesManager(baseFactory)).run();
       /*
        Thread t  = new Thread(new GhcnCountriesManager(baseFactory));
        t.start();
        try {
            t.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        */
    }

    public void updateStations() {
        ThreadPoolManager.addTask(new GhcnStationsManager(baseFactory));
        //(new GhcnStationsManager(baseFactory)).run();
       /*
        Thread t  = new Thread(new GhcnStationsManager(baseFactory));
        t.start();
        try {
            t.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        */
    }

    public void updateUSStates() {
        ThreadPoolManager.addTask(new GhcnUSStatesManager(baseFactory));
        //(new GhcnUSStatesManager(baseFactory)).run();
        /*
        Thread t  = new Thread(new GhcnUSStatesManager(baseFactory));
        t.start();
        try {
            t.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        */
    }

    public void updateDaily() {
        ThreadPoolManager.addTask(new GhcnDailyManager(baseFactory));
    }
}

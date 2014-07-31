package org.kevoree.modeling.test.ghcn;

import kmf.ghcn.DataSet;
import kmf.ghcn.factory.*;
import org.kevoree.modeling.api.TransactionManager;
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
    private GhcnTransactionManager tm;

    public GhcnLoader() {
        initFactory();
    }


    public GhcnLoader(String dbLocation) {
        this.dbLocation=dbLocation;
        initFactory();
    }

    private void initFactory() {

        tm = new GhcnTransactionManager(new LevelDbDataStore(dbLocation));
        checkRoot();

    }


    private void checkRoot() {
        try {

            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMd");
            simpleDateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));

            GhcnTransaction transaction = tm.createTransaction();
            GhcnTimeView rootTimeView = transaction.time(simpleDateFormat.parse("18000101").getTime());
            DataSet root = (DataSet)rootTimeView.lookup("/");
            if(root == null) {
                System.out.println("Create root");
                root = rootTimeView.createDataSet();
                rootTimeView.root(root);
                transaction.commit();
                transaction.close();
            }
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }


    public void free(){
        tm.close();
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

    public void updateCountries() {
        ThreadPoolManager.addTask(new GhcnCountriesManager(tm));
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
        ThreadPoolManager.addTask(new GhcnStationsManager(tm));
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
        ThreadPoolManager.addTask(new GhcnUSStatesManager(tm));
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
        ThreadPoolManager.addTask(new GhcnDailyManager(tm));
    }
}

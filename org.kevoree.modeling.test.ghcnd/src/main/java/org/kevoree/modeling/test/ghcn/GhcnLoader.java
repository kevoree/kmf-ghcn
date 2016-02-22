package org.kevoree.modeling.test.ghcn;

import kmf.ghcn.GhcndModel;
import org.kevoree.modeling.KCallback;
import org.kevoree.modeling.memory.manager.DataManagerBuilder;
import org.kevoree.modeling.plugin.LevelDBPlugin;
import org.kevoree.modeling.test.ghcn.utils.Stats;
import org.kevoree.modeling.test.ghcn.utils.ThreadPoolManager;
import org.kevoree.modeling.test.ghcn.utils.UpdateResult;

import java.io.IOException;
import java.util.concurrent.*;

/**
 * Created by gregory.nain on 22/07/2014.
 */
public class GhcnLoader {

    private String dbLocation = "GhcnLevelDB";
    private GhcndModel model;

    public GhcnLoader() {
    }


    public GhcnLoader(String dbLocation) {
        this.dbLocation = dbLocation;
    }

    public void initFactory(final KCallback nextTask) {

        try {

            model = new GhcndModel(DataManagerBuilder.create().withContentDeliveryDriver(new LevelDBPlugin(dbLocation)).build());
            model.connect(new KCallback() {
                public void on(Object o) {
                    nextTask.on(null);
                }
            });

        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public void free() {

    }

    public void updateAll() {
        updateCountries();
        updateUSStates();
        updateStations();
        updateDaily();
    }

    public void waitCompletion() {
        ThreadPoolManager.shutdown();
        while (ThreadPoolManager.waitingExecution.size() > 0) {
            try {
                Future<UpdateResult> f = ThreadPoolManager.waitingExecution.poll();
                UpdateResult result = f.get();
                for (Stats s : result.statistics) {
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
        System.out.println("Starting Country update");
        ThreadPoolManager.addTask(new GhcnCountriesManager(model));
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
        ThreadPoolManager.addTask(new GhcnStationsManager(model));
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
        ThreadPoolManager.addTask(new GhcnUSStatesManager(model));
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
        ThreadPoolManager.addTask(new GhcnDailyManager(model));
    }
}

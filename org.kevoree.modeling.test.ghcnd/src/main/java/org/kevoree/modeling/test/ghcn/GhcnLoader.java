package org.kevoree.modeling.test.ghcn;

import kmf.ghcn.DataSet;
import kmf.ghcn.GhcndModel;
import kmf.ghcn.GhcndUniverse;
import kmf.ghcn.GhcndView;
import org.kevoree.modeling.KCallback;
import org.kevoree.modeling.KObject;
import org.kevoree.modeling.drivers.leveldb.LevelDbContentDeliveryDriver;
import org.kevoree.modeling.memory.manager.DataManagerBuilder;
import org.kevoree.modeling.scheduler.impl.DirectScheduler;
import org.kevoree.modeling.test.ghcn.utils.Stats;
import org.kevoree.modeling.test.ghcn.utils.ThreadPoolManager;
import org.kevoree.modeling.test.ghcn.utils.UpdateResult;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.TimeZone;
import java.util.concurrent.*;

/**
 * Created by gregory.nain on 22/07/2014.
 */
public class GhcnLoader {

    private String dbLocation = "GhcnLevelDB";
    private GhcndUniverse universe;

    public GhcnLoader() {
    }


    public GhcnLoader(String dbLocation) {
        this.dbLocation = dbLocation;
    }

    public void initFactory(final KCallback nextTask) {

        try {

            final GhcndModel model = new GhcndModel(DataManagerBuilder.create().withScheduler(new DirectScheduler()).withContentDeliveryDriver(new LevelDbContentDeliveryDriver(dbLocation)).build());
            model.connect(new KCallback() {
                public void on(Object o) {
                    universe = model.universe(0);
                    checkRoot(nextTask);
                }
            });

        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    private void checkRoot(final KCallback nextTask) {

        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMd");
        simpleDateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));

        try {
            final GhcndView rootTimeView = universe.time(simpleDateFormat.parse("18000101").getTime());
            rootTimeView.getRoot(new KCallback<KObject>() {
                public void on(KObject kObject) {
                    if (kObject == null) {
                        System.out.println("Create root");
                        final DataSet root = rootTimeView.createDataSet();
                        rootTimeView.setRoot(root, new KCallback() {
                            public void on(Object o) {
                                root.manager().save(new KCallback<Throwable>() {
                                    public void on(Throwable throwable) {
                                        if (throwable != null) {
                                            throwable.printStackTrace();
                                        } else {
                                            System.out.println("Root saved");
                                        }
                                        nextTask.on(null);
                                    }
                                });
                            }
                        });
                    } else {
                        nextTask.on(null);
                    }
                }
            });
        } catch (ParseException e) {
            e.printStackTrace();
        }

    }


    public void free() {
        universe = null;
    }

    public void updateAll() {
        updateCountries();
        updateUSStates();
        updateStations();
        updateDaily();
    }

    public void waitCompletion() {
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
        ThreadPoolManager.addTask(new GhcnCountriesManager(universe));
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
        ThreadPoolManager.addTask(new GhcnStationsManager(universe));
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
        ThreadPoolManager.addTask(new GhcnUSStatesManager(universe));
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
        ThreadPoolManager.addTask(new GhcnDailyManager(universe));
    }
}

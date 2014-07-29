package org.kevoree.modeling.test.ghcn;

import kmf.ghcn.DataSet;
import kmf.ghcn.factory.DefaultGhcnFactory;
import kmf.ghcn.factory.GhcnFactory;
import org.kevoree.modeling.api.time.TimeView;
import org.kevoree.modeling.datastores.leveldb.LevelDbDataStore;
import org.kevoree.modeling.test.ghcn.utils.Stats;
import org.kevoree.modeling.test.ghcn.utils.ThreadPoolManager;
import org.kevoree.modeling.test.ghcn.utils.UpdateResult;
import java.util.concurrent.*;

/**
 * Created by gregory.nain on 22/07/2014.
 */
public class GhcnLoader {

    private String dbLocation = "GhcnLeveDB";
    private GhcnFactory baseFactory;

    public void initFactory() {

        baseFactory = new DefaultGhcnFactory();
        baseFactory.setDatastore(new LevelDbDataStore(dbLocation));
        TimeView<GhcnFactory> rootTimeView = baseFactory.time("0");
        DataSet root = (DataSet)rootTimeView.lookup("/");
        if(root == null) {
            root = rootTimeView.factory().createDataSet();
            rootTimeView.root(root);
            rootTimeView.commit();
        }
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
    }

    public void updateCountries() {
        ThreadPoolManager.addTask(new GhcnCountriesManager(baseFactory));
    }

    public void updateStations() {
        ThreadPoolManager.addTask(new GhcnStationsManager(baseFactory));
    }

    public void updateUSStates() {
        ThreadPoolManager.addTask(new GhcnUSStatesManager(baseFactory));
    }

    public void updateDaily() {
        ThreadPoolManager.addTask(new GhcnDailyManager(baseFactory));
    }
}

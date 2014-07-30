package org.kevoree.modeling.test.ghcn;

import kmf.ghcn.*;
import kmf.ghcn.factory.DefaultGhcnFactory;
import kmf.ghcn.factory.GhcnFactory;
import org.kevoree.modeling.api.time.TimePoint;
import org.kevoree.modeling.api.time.TimeSegmentConst;
import org.kevoree.modeling.api.time.TimeView;
import org.kevoree.modeling.api.time.blob.Node;
import org.kevoree.modeling.api.time.blob.RBTree;
import org.kevoree.modeling.api.time.blob.TimeMeta;
import org.kevoree.modeling.datastores.leveldb.LevelDbDataStore;
import org.kevoree.modeling.test.ghcn.utils.Stats;
import org.kevoree.modeling.test.ghcn.utils.ThreadPoolManager;
import org.kevoree.modeling.test.ghcn.utils.UpdateResult;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * Created by gregory.nain on 22/07/2014.
 */
public class GhcnReader {

    private String dbLocation = "GhcnLevelDB";
    private GhcnFactory baseFactory;

    public GhcnReader() {
        initFactory();
    }


    public GhcnReader(String dbLocation) {
        this.dbLocation=dbLocation;
        initFactory();
    }

    private void initFactory() {

        baseFactory = new DefaultGhcnFactory();
        baseFactory.setDatastore(new LevelDbDataStore(dbLocation));
        //TimeView<GhcnFactory> rootTimeView = baseFactory.time("0");
        //DataSet root = (DataSet)rootTimeView.lookup("/");
        DataSet root = (DataSet)baseFactory.lookup("/");
        if(root == null) {
            System.err.println("Root not found !");
            System.exit(-1);
            /*
            root = rootTimeView.factory().createDataSet();
            rootTimeView.root(root);
            rootTimeView.commit();
            */
        }

        //System.out.println(baseFactory.createJSONSerializer().serialize(baseFactory.lookup("/")));
    }


    public void printCountries() {
        //TimeView<GhcnFactory> rootTimeView = baseFactory.time("0");
        //DataSet root = (DataSet)rootTimeView.lookup("/");
        DataSet root = (DataSet)baseFactory.lookup("/");
        int i = 0;
        for(Country c : root.getCountries()) {
            i++;
            if(i % 100 == 0){
                System.out.println(c.getName());
            } else {
                System.out.print(c.getName() + ", ");
            }
        }
        System.out.println();
    }


    public void printStates() {
        //TimeView<GhcnFactory> rootTimeView = baseFactory.time("0");
        //DataSet root = (DataSet)rootTimeView.lookup("/");
        DataSet root = (DataSet)baseFactory.lookup("/");
        int i = 0;
        for(USState state : root.getUsStates()) {
            i++;
            if(i % 100 == 0){
                System.out.println(state.getName());
            } else {
                System.out.print(state.getName() + ", ");
            }
        }
        System.out.println();
    }

    public void printStations() {
        //TimeView<GhcnFactory> rootTimeView = baseFactory.time("0");
        //DataSet root = (DataSet)rootTimeView.lookup("/");
        DataSet root = (DataSet)baseFactory.lookup("/");
        int i = 0;
        for(Station station : root.getStations()) {
            i++;
            if(i % 100 == 0){
                System.out.println(station.getName());
            } else {
                System.out.print(station.getName() + ", ");
            }
        }
        System.out.println();
    }


    public void printDailyRecords() {

    }

    public void printDates() {
        TimeMeta timeMetaRoot = baseFactory.getTimeTree("#global");
        Node timeTreeRoot = timeMetaRoot.getVersionTree().getRoot();
        rbPrinter(timeTreeRoot);

    }

    private void rbPrinter(Node tree) {
        if(tree.getLeft() != null) {
            rbPrinter(tree.getLeft());
        }
        if(tree.getKey() != null) {
            System.out.println("" + SimpleDateFormat.getDateInstance().format(new Date(tree.getKey().getTimestamp())));
        }
        if(tree.getRight() != null) {
            rbPrinter(tree.getRight());
        }
    }


}

package org.kevoree.modeling.test.ghcn;

import jet.runtime.typeinfo.JetValueParameter;
import kmf.ghcn.*;
import kmf.ghcn.factory.*;
import org.jetbrains.annotations.NotNull;
import org.kevoree.modeling.api.time.TimeAwareKMFFactory;
import org.kevoree.modeling.api.time.TimeWalker;
import org.kevoree.modeling.api.time.blob.TimeMeta;
import org.kevoree.modeling.datastores.leveldb.LevelDbDataStore;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

/**
 * Created by gregory.nain on 22/07/2014.
 */
public class GhcnReader {

    private String dbLocation = "GhcnLevelDB";
    private GhcnTransactionManager tm;
    private SimpleDateFormat simpleDateFormat;

    public GhcnReader() {
        initFactory();
    }


    public GhcnReader(String dbLocation) {
        this.dbLocation=dbLocation;
        initFactory();
    }

    private void initFactory() {

        try {
            tm = new GhcnTransactionManager(new LevelDbDataStore(dbLocation));

            simpleDateFormat = new SimpleDateFormat("yyyyMMd");
            simpleDateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));

            GhcnTransaction transaction = tm.createTransaction();
            GhcnTimeView rootTimeView = transaction.time(simpleDateFormat.parse("18000101").getTime());
            DataSet root = (DataSet)rootTimeView.lookup("/");
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
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }


    public void printCountries() {
        try {
            GhcnTransaction transaction = tm.createTransaction();
            GhcnTimeView rootTimeView = null;
            rootTimeView = transaction.time(simpleDateFormat.parse("18000101").getTime());
            DataSet root = (DataSet)rootTimeView.lookup("/");
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
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }


    public void printStates() {
        try {
            GhcnTransaction transaction = tm.createTransaction();
            GhcnTimeView rootTimeView = null;
            rootTimeView = transaction.time(simpleDateFormat.parse("18000101").getTime());
            DataSet root = (DataSet)rootTimeView.lookup("/");
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
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }

    public void printStations() {
        try {
            GhcnTransaction transaction = tm.createTransaction();
            GhcnTimeView rootTimeView = null;
            rootTimeView = transaction.time(simpleDateFormat.parse("18000101").getTime());
            DataSet root = (DataSet)rootTimeView.lookup("/");
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
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }


    public void printDailyRecords() {

    }

    public void printDates() {
        try {
            GhcnTransaction transaction = tm.createTransaction();
            final GhcnTimeView rootTimeView = transaction.time(simpleDateFormat.parse("18000101").getTime());
            final TimeMeta timeMetaRoot = ((TimeAwareKMFFactory)rootTimeView).getTimeTree("#global");
            timeMetaRoot.walkAsc(new TimeWalker() {
                long previous;
                @Override
                public void walk(@JetValueParameter(name = "timePoint") @NotNull long timePoint) {
                    System.out.println("" + SimpleDateFormat.getDateInstance().format(new Date(timePoint)));
                }
            });

        } catch (ParseException e) {
            e.printStackTrace();
        }

    }

}

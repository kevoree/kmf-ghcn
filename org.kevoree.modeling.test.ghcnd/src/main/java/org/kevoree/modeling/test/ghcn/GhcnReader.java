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
    private GhcnTransaction transaction;

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

            transaction = tm.createTransaction();
            GhcnTimeView rootTimeView = transaction.time(simpleDateFormat.parse("18000101").getTime());
            DataSet root = (DataSet)rootTimeView.lookup("/");
            if(root == null) {
                System.err.println("Root not found !");
                System.exit(-1);
            }
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }


    public void printCountries() {
        try {
            GhcnTimeView rootTimeView = transaction.time(simpleDateFormat.parse("18000101").getTime());
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
            GhcnTimeView rootTimeView = transaction.time(simpleDateFormat.parse("18000101").getTime());
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
        try {
            final int[] records = new int[1];
            final GhcnTimeView rootTimeView = transaction.time(simpleDateFormat.parse("18000101").getTime());
            for(final Station station : ((DataSet)rootTimeView.lookup("/")).getStations()) {
                //System.out.println("Station:" + station.getName());
                if(station.next() != null) {
                    GhcnTransaction stationTransaction = tm.createTransaction();
                    GhcnTimeView tv = stationTransaction.time(station.next().getNow());

                    Station temporizedStation = (Station) tv.lookup(station.path());

                    for (final Record rc : temporizedStation.getLastRecords()) {
                        final GhcnTransaction recordTransaction = tm.createTransaction();
                        GhcnTimeView recordView = recordTransaction.time(rc.getNow());
                        Record timedRecord = (Record) recordView.lookup(rc.path());

                        /*
                        TimeMeta recordTimes = ((TimeAwareKMFFactory)recordView).getTimeTree(timedRecord.path());
                        recordTimes.walkAsc(new TimeWalker() {
                            @Override
                            public void walk(@JetValueParameter(name = "timePoint") long l) {
                                records[0]++;
                                GhcnTimeView recordView = recordTransaction.time(l);
                                Record timedRecord = (Record) recordView.lookup(rc.path());
                                StringBuilder sb = new StringBuilder();
                                sb.append("Recorded on ");
                                sb.append(simpleDateFormat.format(timedRecord.getNow()));
                                sb.append(" on Station ");
                                sb.append(station.getName() + "("+station.getId()+")");
                                sb.append("[");
                                sb.append("Type:");
                                sb.append(Element.parse(timedRecord.getType()).name);
                                sb.append(", Quality:");
                                sb.append(Quality.parse(timedRecord.getQuality()).name);
                                sb.append(", Measurement:");
                                sb.append(Measurement.parse(timedRecord.getMeasurement()).name);
                                sb.append(", Source:");
                                sb.append(Source.parse(timedRecord.getSource()).name);
                                sb.append(", Value:");
                                sb.append(timedRecord.getValue());
                                sb.append("]");
                                System.out.println(sb.toString());
                            }
                        });
                        */

                        do {
                            //timedRecord = (Record) recordView.lookup(timedRecord.path());
                            StringBuilder sb = new StringBuilder();
                            sb.append("Recorded on ");
                            sb.append(simpleDateFormat.format(timedRecord.getNow()));
                            sb.append(" on Station ");
                            sb.append(station.getName() + "("+station.getId()+")");
                            sb.append("[");
                            sb.append("Type:");
                            sb.append(Element.parse(timedRecord.getType()).name);
                            sb.append(", Quality:");
                            sb.append(Quality.parse(timedRecord.getQuality()).name);
                            sb.append(", Measurement:");
                            sb.append(Measurement.parse(timedRecord.getMeasurement()).name);
                            sb.append(", Source:");
                            sb.append(Source.parse(timedRecord.getSource()).name);
                            sb.append(", Value:");
                            sb.append(timedRecord.getValue());
                            sb.append("]");
                            System.out.println(sb.toString());
                            records[0]++;
                            timedRecord = timedRecord.next();
                        } while (timedRecord != null);

                        recordTransaction.close();
                    }
                    stationTransaction.close();
                }
            }
            System.out.println("Records:" + records[0]);

        } catch (ParseException e) {
            e.printStackTrace();
        }
    }

    public void printDates() {
        try {
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

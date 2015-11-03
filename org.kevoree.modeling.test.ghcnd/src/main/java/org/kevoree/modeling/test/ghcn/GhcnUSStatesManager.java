package org.kevoree.modeling.test.ghcn;

import kmf.ghcn.DataSet;
import kmf.ghcn.GhcndUniverse;
import kmf.ghcn.meta.MetaDataSet;
import kmf.ghcn.meta.MetaUSState;
import org.kevoree.modeling.KCallback;
import org.kevoree.modeling.KObject;
import org.kevoree.modeling.test.ghcn.utils.MyFtpClient;
import org.kevoree.modeling.test.ghcn.utils.Stats;

import java.io.*;
import java.text.ParseException;
import java.util.ArrayList;

/**
 * Created by gregory.nain on 23/07/2014.
 */
public class GhcnUSStatesManager extends AbstractManager {

    protected static String serverAddress = "ftp.ncdc.noaa.gov";
    protected static String remoteDirectory = "/pub/data/ghcn/daily";
    protected static String localDirectory = "/tmp/ghcn";
    protected static String fileName = "ghcnd-states.txt";

    public GhcnUSStatesManager(GhcndUniverse universe) {
        super(universe);
    }

    public void run() {
        final Stats stats = new Stats(getClass().getSimpleName());

        try {
            this.rootTimeView = universe.time(simpleDateFormat.parse("18000101").getTime());
        } catch (ParseException e) {
            e.printStackTrace();
        }
        rootTimeView.getRoot(new KCallback<KObject>() {
            public void on(KObject results) {
                MyFtpClient ftp = null;
                BufferedReader reader = null;
                if (results == null) {
                    System.err.println("Could not reach the root");
                }
                try {
                    root = (DataSet) results;
                    if (root != null) {
                        ftp = new MyFtpClient(serverAddress, remoteDirectory, localDirectory, null, null);
                        System.out.println("Downloading :" + remoteDirectory + "/" + fileName);
                        long startTime = System.currentTimeMillis();
                        File countriesFile = ftp.getRemoteFile(remoteDirectory, fileName);
                        stats.time_download = System.currentTimeMillis() - startTime;
                        System.out.println("Downloading Complete:" + countriesFile.getAbsolutePath());

                        if (countriesFile != null && countriesFile.exists()) {
                            reader = new BufferedReader(new FileReader(countriesFile));
                            String line = null;
                            ArrayList<String> lines = new ArrayList<String>();
                            startTime = System.currentTimeMillis();
                            while ((line = reader.readLine()) != null) {
                                lines.add(line);
                            }
                            stats.time_readFile = System.currentTimeMillis() - startTime;
                            startTime = System.currentTimeMillis();
                            for (String l : lines) {
                                processLine(l, stats);
                                if (stats.insertions != 0 && stats.insertions % 50 == 0) {
                                    System.out.println("Inserted " + stats.insertions + "/" + lines.size());
                                }
                            }
                            stats.time_insert = System.currentTimeMillis() - startTime;
                            startTime = System.currentTimeMillis();

                            root.manager().save(new KCallback<Throwable>() {
                                public void on(Throwable throwable) {
                                    if (throwable != null) {
                                        throwable.printStackTrace();
                                    }
                                }
                            });
                            //baseFactory.commit();
                            stats.time_commit = System.currentTimeMillis() - startTime;
                        } else {
                            System.err.println("Country file not available locally !");
                        }
                    }


                    result.statistics.add(stats);

                } catch (FileNotFoundException e) {
                    e.printStackTrace();
                } catch (IOException e) {
                    e.printStackTrace();
                } finally {
                    if (ftp != null) {
                        ftp.disconnect();
                    }
                    if (reader != null) {
                        try {
                            reader.close();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                }
            }
        });
    }


    /*
    * CODE          1-2    Character
    * NAME         4-50    Character
    * */

    private void processLine(String line, final Stats stats) {
        //System.out.println("Line("+line.length()+"):" + line);
        final String id = line.substring(0, 2);
        final String name = line.substring(3, line.length()).trim();
        //System.out.println("ID:"+id+"; name:" + name);
        stats.lookups++;

        root.traversal().traverse(MetaDataSet.REL_USSTATES).withAttribute(MetaUSState.ATT_ID, id).then(new KCallback<KObject[]>() {
            public void on(KObject[] states) {
                if (states == null || states.length == 0) {
                    root.addUsStates(rootTimeView.createUSState().setId(id).setName(name));
                    stats.insertions++;
                }
            }
        });

    }
}




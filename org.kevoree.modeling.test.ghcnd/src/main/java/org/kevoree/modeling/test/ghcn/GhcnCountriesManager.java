package org.kevoree.modeling.test.ghcn;

import kmf.ghcn.Country;
import kmf.ghcn.DataSet;
import kmf.ghcn.GhcndUniverse;
import kmf.ghcn.meta.MetaCountry;
import kmf.ghcn.meta.MetaDataSet;
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
public class GhcnCountriesManager extends AbstractManager {


    protected static String serverAddress = "ftp.ncdc.noaa.gov";
    protected static String remoteDirectory = "/pub/data/ghcn/daily";
    protected static String localDirectory = "/tmp/ghcn";
    protected static String fileName = "ghcnd-countries.txt";


    public GhcnCountriesManager(GhcndUniverse universe) {
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
                    System.out.println("Could not reach the root");
                } else {
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
                    } catch (IOException e) {
                        e.printStackTrace();
                    } finally {
                        result.statistics.add(stats);
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
            }
        });


    }


    /*
    * CODE          1-2    Character
    * NAME         4-50    Character
    * */

    private void processLine(String line, final Stats stats) {
        final String id = line.substring(0, 2);
        final String name = line.substring(3, line.length()).trim();
        stats.lookups++;
        root.traversal().traverse(MetaDataSet.REL_COUNTRIES).withAttribute(MetaCountry.ATT_ID, id).then(new KCallback<KObject[]>() {
            public void on(KObject[] results) {
                if (results == null || results.length == 0) {
                    Country c = rootTimeView.createCountry().setId(id).setName(name);
                    root.addCountries(c);
                    stats.insertions++;
                }
            }
        });
    }


}

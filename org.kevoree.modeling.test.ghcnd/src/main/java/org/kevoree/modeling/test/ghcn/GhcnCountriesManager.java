package org.kevoree.modeling.test.ghcn;

import kmf.ghcn.Country;
import kmf.ghcn.DataSet;
import kmf.ghcn.factory.GhcnFactory;
import kmf.ghcn.factory.GhcnTransaction;
import kmf.ghcn.factory.GhcnTransactionManager;
import org.kevoree.modeling.test.ghcn.utils.FtpClient;
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


    public GhcnCountriesManager(GhcnTransactionManager tm) {
        super(tm);
    }



    public void run() {
        FtpClient ftp = null;
        BufferedReader reader = null;
        try {
            GhcnTransaction transaction = tm.createTransaction();
            this.rootTimeView =  transaction.time(simpleDateFormat.parse("18000101").getTime());
            //root = (DataSet)baseFactory.lookup("/");
            root = (DataSet)rootTimeView.lookup("/");

            if(root == null) {
                System.err.println("Could not reach the root");
            }

            Stats stats = new Stats(getClass().getSimpleName());

            if(root != null) {
                ftp = new FtpClient(serverAddress, remoteDirectory, localDirectory, null, null);
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
                    for(String l : lines) {
                        processLine(l, stats);
                        if( stats.insertions != 0 && stats.insertions % 50 == 0){
                            System.out.println("Inserted " + stats.insertions + "/" + lines.size());
                        }
                    }
                    stats.time_insert = System.currentTimeMillis() - startTime;
                    startTime = System.currentTimeMillis();
                    transaction.commit();
                    transaction.close();
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
        } catch (ParseException e) {
            e.printStackTrace();
        } finally {
            if(ftp != null) {
                ftp.disconnect();
            }
            if(reader != null) {
                try {
                    reader.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

        }
    }


    /*
    * CODE          1-2    Character
    * NAME         4-50    Character
    * */
    private void processLine(String line, Stats stats) {
        String id = line.substring(0,2);
        String name = line.substring(3,line.length()).trim();
        stats.lookups++;
        /*
        if(baseFactory.lookup("/countries["+id+"]") == null) {
            Country c = baseFactory.createCountry().withId(id).withName(name);
            root.addCountries(c);
            stats.insertions++;
        }
        */
        if(rootTimeView.lookup("/countries["+id+"]") == null) {
            Country c = rootTimeView.createCountry().withId(id).withName(name);
            root.addCountries(c);
            stats.insertions++;
        }
    }




}

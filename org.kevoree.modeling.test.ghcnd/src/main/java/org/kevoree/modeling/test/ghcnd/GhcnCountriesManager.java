package org.kevoree.modeling.test.ghcnd;

import kmf.ghcnd.Country;
import kmf.ghcnd.DataSet;
import kmf.ghcnd.GhcndFactory;
import org.kevoree.modeling.api.time.TimeView;
import org.kevoree.modeling.test.ghcnd.utils.FtpClient;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * Created by gregory.nain on 23/07/2014.
 */
public class GhcnCountriesManager {

    protected TimeView<GhcndFactory> rootTimeView;
    //protected GhcndFactory factory;
    protected DataSet root;
    protected static String serverAddress = "ftp.ncdc.noaa.gov";
    protected static String remoteDirectory = "/pub/data/ghcn/daily";
    protected static String localDirectory = "/tmp/ghcn";
    protected static String fileName = "ghcnd-countries.txt";

    private int insertions = 0;
    private int lookups = 0;
    private long time_download = 0L;
    private long time_readFile = 0L;
    private long time_insert = 0L;
    private long time_commit = 0L;

    public GhcnCountriesManager(GhcndFactory factory) {

        this.rootTimeView =  factory.time("0");
        //this.factory = factory;
        root = (DataSet)rootTimeView.lookup("/");
        if(root == null) {
            System.err.println("Could not reach the root");
        }

    }

    public void updateCountries() {
        try {
            if(root != null) {
                FtpClient ftp = new FtpClient(serverAddress, remoteDirectory, localDirectory, null, null);
                System.out.println("Downloading :" + remoteDirectory + "/" + fileName);
                long startTime = System.currentTimeMillis();
                File countriesFile = ftp.getRemoteFile(remoteDirectory, fileName);
                time_download = System.currentTimeMillis() - startTime;
                System.out.println("Downloading Complete:" + countriesFile.getAbsolutePath());

                if (countriesFile != null && countriesFile.exists()) {
                    BufferedReader reader = new BufferedReader(new FileReader(countriesFile));
                    String line = null;
                    ArrayList<String> lines = new ArrayList<String>();
                    startTime = System.currentTimeMillis();
                    while ((line = reader.readLine()) != null) {
                        lines.add(line);
                    }
                    time_readFile = System.currentTimeMillis() - startTime;
                    startTime = System.currentTimeMillis();
                    for(String l : lines) {
                        processLine(l);
                        if( insertions != 0 && insertions % 50 == 0){
                            System.out.println("Inserted " + insertions + "/" + lines.size());
                        }
                    }
                    time_insert = System.currentTimeMillis() - startTime;
                    startTime = System.currentTimeMillis();
                    rootTimeView.commit();
                    //factory.commit();
                    time_commit = System.currentTimeMillis() - startTime;
                } else {
                    System.err.println("Country file not available locally !");
                }
            }

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }



    /*
    * CODE          1-2    Character
    * NAME         4-50    Character
    * */
    private void processLine(String line) {
        String id = line.substring(0,2);
        String name = line.substring(3,line.length()).trim();
        lookups++;
        if(rootTimeView.lookup("/countries["+id+"]") == null) {
        Country c = rootTimeView.factory().createCountry().withId(id).withName(name);
        //if(factory.lookup("/countries["+id+"]") == null) {
        //Country c = factory.createCountry().withId(id).withName(name);
            root.addCountries(c);
            insertions++;
        }
    }

    @Override
    public String toString() {
        return "[CountriesManager] Stats:: Insertions:" + insertions + " Lookups:" + lookups
                + " DownloadTime:" + time_download + "ms"
                + " ReadFileTime:" + time_readFile + "ms"
                + " InsertionTime:" + time_insert + "ms"
                + " CommitTime:" + time_commit + "ms";
    }
}

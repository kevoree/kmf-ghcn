package org.kevoree.modeling.test.ghcn;

import kmf.ghcn.factory.GhcnFactory;
import org.kevoree.modeling.test.ghcn.utils.FtpClient;
import org.kevoree.modeling.test.ghcn.utils.Stats;

import java.io.*;
import java.util.ArrayList;

/**
 * Created by gregory.nain on 23/07/2014.
 */
public class GhcnUSStatesManager extends AbstractManager {

    protected static String serverAddress = "ftp.ncdc.noaa.gov";
    protected static String remoteDirectory = "/pub/data/ghcn/daily";
    protected static String localDirectory = "/tmp/ghcn";
    protected static String fileName = "ghcnd-states.txt";

    public GhcnUSStatesManager(GhcnFactory factory) {
        super(factory);
    }

    public void run() {
        FtpClient ftp = null;
        BufferedReader reader = null;
        Stats stats = new Stats(getClass().getSimpleName());
        try {
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
                    //rootTimeView.commit();
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
        //System.out.println("Line("+line.length()+"):" + line);
        String id = line.substring(0,2);
        String name = line.substring(3,line.length()).trim();
        //System.out.println("ID:"+id+"; name:" + name);
        stats.lookups++;
        if(rootTimeView.lookup("/usStates["+id+"]") == null) {
            root.addUsStates(rootTimeView.factory().createUSState().withId(id).withName(name));
            // if(factory.lookup("/usStates["+id+"]") == null) {
            //     root.addUsStates(factory.createUSState().withId(id).withName(name));
            stats.insertions++;
        }
    }

}




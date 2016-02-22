package org.kevoree.modeling.test.ghcn;

import kmf.ghcn.GhcndModel;
import kmf.ghcn.meta.MetaCountry;
import org.kevoree.modeling.KCallback;
import org.kevoree.modeling.KConfig;
import org.kevoree.modeling.KObject;
import org.kevoree.modeling.test.ghcn.utils.MyFtpClient;
import org.kevoree.modeling.test.ghcn.utils.Stats;

import java.io.*;
import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;

/**
 * Created by gregory.nain on 23/07/2014.
 */
public class GhcnCountriesManager extends AbstractManager {


    protected static String serverAddress = "ftp.ncdc.noaa.gov";
    protected static String remoteDirectory = "/pub/data/ghcn/daily";
    protected static String localDirectory = "/tmp/ghcn";
    protected static String fileName = "ghcnd-countries.txt";

    private CountDownLatch latch;


    public GhcnCountriesManager(GhcndModel model) {
        super(model);
    }


    public void run() {

        final Stats stats = new Stats(getClass().getSimpleName());

        try {
            MyFtpClient ftp = new MyFtpClient(serverAddress, remoteDirectory, localDirectory, null, null);
            System.out.println("Downloading :" + remoteDirectory + "/" + fileName);
            long startTime = System.currentTimeMillis();
            File countriesFile = ftp.getRemoteFile(remoteDirectory, fileName);
            stats.time_download = System.currentTimeMillis() - startTime;
            System.out.println("Downloading Complete:" + countriesFile.getAbsolutePath());

            if (countriesFile != null && countriesFile.exists()) {
                BufferedReader reader = null;
                reader = new BufferedReader(new FileReader(countriesFile));

                String line = null;
                ArrayList<String> lines = new ArrayList<String>();
                startTime = System.currentTimeMillis();
                while ((line = reader.readLine()) != null) {
                    lines.add(line);
                }
                latch = new CountDownLatch(lines.size());
                System.out.println(lines.size() + " lines in file.");
                stats.time_readFile = System.currentTimeMillis() - startTime;
                startTime = System.currentTimeMillis();
                for (String l : lines) {
                    processLine(l, stats);
                }
                latch.await();
                stats.time_insert = System.currentTimeMillis() - startTime;
                startTime = System.currentTimeMillis();

                model.save(new KCallback() {
                    public void on(Object o) {
                        if(o!=null && o instanceof Throwable) {
                            ((Throwable)o).printStackTrace();
                        }
                    }
                });
                stats.time_commit = System.currentTimeMillis() - startTime;
            } else {
                System.err.println("Country file not available locally !");
            }
            result.statistics.add(stats);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }


    /*
    * CODE          1-2    Character
    * NAME         4-50    Character
    * */

    private void processLine(String line, final Stats stats) {
        final String id = line.substring(0, 2);
        final String name = line.substring(3, line.length()).trim();
        stats.lookups++;

        model.find(MetaCountry.getInstance(), 0, KConfig.BEGINNING_OF_TIME, "id=" + id, new KCallback<KObject>() {
            public void on(KObject kObject) {
                if (kObject == null) {
                    model.createCountry(0, KConfig.BEGINNING_OF_TIME).setId(id).setName(name);
                    stats.insertions++;
                    if (stats.insertions != 0 && stats.insertions % 50 == 0) {
                        System.out.println("Inserted " + stats.insertions + " so far.");
                    }

                }
                latch.countDown();
            }
        });

    }


}

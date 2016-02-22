package org.kevoree.modeling.test.ghcn;

import kmf.ghcn.GhcndModel;
import kmf.ghcn.meta.MetaUSState;
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
public class GhcnUSStatesManager extends AbstractManager {

    protected static String serverAddress = "ftp.ncdc.noaa.gov";
    protected static String remoteDirectory = "/pub/data/ghcn/daily";
    protected static String localDirectory = "/tmp/ghcn";
    protected static String fileName = "ghcnd-states.txt";

    private CountDownLatch latch;

    public GhcnUSStatesManager(GhcndModel model) {
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
                BufferedReader reader = new BufferedReader(new FileReader(countriesFile));
                String line = null;
                ArrayList<String> lines = new ArrayList<String>();
                startTime = System.currentTimeMillis();
                while ((line = reader.readLine()) != null) {
                    lines.add(line);
                }
                latch = new CountDownLatch(lines.size());
                stats.time_readFile = System.currentTimeMillis() - startTime;
                startTime = System.currentTimeMillis();
                for (String l : lines) {
                    processLine(l, stats);

                }
                latch.await();
                stats.time_insert = System.currentTimeMillis() - startTime;
                startTime = System.currentTimeMillis();

                model.save(new KCallback<Throwable>() {
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

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        result.statistics.add(stats);


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

        model.find(MetaUSState.getInstance(), 0, KConfig.BEGINNING_OF_TIME, "id=" + id, new KCallback<KObject>() {
            public void on(KObject kObject) {
                if (kObject == null) {
                    model.createUSState(0, KConfig.BEGINNING_OF_TIME).setId(id).setName(name);
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




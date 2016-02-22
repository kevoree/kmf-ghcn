package org.kevoree.modeling.test.ghcn;

import kmf.ghcn.*;
import kmf.ghcn.meta.MetaCountry;
import kmf.ghcn.meta.MetaStation;
import kmf.ghcn.meta.MetaUSState;
import org.kevoree.modeling.KCallback;
import org.kevoree.modeling.KConfig;
import org.kevoree.modeling.KObject;
import org.kevoree.modeling.defer.KDefer;
import org.kevoree.modeling.test.ghcn.utils.MyFtpClient;
import org.kevoree.modeling.test.ghcn.utils.Stats;

import java.io.*;
import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;

/**
 * Created by gregory.nain on 23/07/2014.
 */
public class GhcnStationsManager extends AbstractManager {

    protected static String serverAddress = "ftp.ncdc.noaa.gov";
    protected static String remoteDirectory = "/pub/data/ghcn/daily";
    protected static String localDirectory = "/tmp/ghcn";
    protected static String fileName = "ghcnd-stations.txt";

    private CountDownLatch latch;

    public GhcnStationsManager(GhcndModel model) {
        super(model);
    }


    public void run() {
        final Stats stats = new Stats(getClass().getSimpleName());

        MyFtpClient ftp = new MyFtpClient(serverAddress, remoteDirectory, localDirectory, null, null);
        System.out.println("Downloading :" + remoteDirectory + "/" + fileName);
        long startTime = System.currentTimeMillis();
        final File countriesFile = ftp.getRemoteFile(remoteDirectory, fileName);
        stats.time_download = System.currentTimeMillis() - startTime;
        System.out.println("Downloading Complete:" + countriesFile.getAbsolutePath());


        if (countriesFile != null && countriesFile.exists()) {

            try {
                BufferedReader reader = new BufferedReader(new FileReader(countriesFile));

                final ArrayList<String> lines = new ArrayList<String>();

                String line;
                while ((line = reader.readLine()) != null) {
                    lines.add(line);
                }
                System.out.println(lines.size() + " lines in file");
                latch = new CountDownLatch(lines.size());
                stats.time_readFile = System.currentTimeMillis() - startTime;


                startTime = System.currentTimeMillis();
                for (String l : lines) {
                    //for(int i = 0; i < 35000; i++) {
                    //processLine(lines.get(i), stats);
                    processLine(l, stats);

                }
                latch.await();
                stats.time_insert = System.currentTimeMillis() - startTime;


                startTime = System.currentTimeMillis();
                model.save(null);
                stats.time_commit = System.currentTimeMillis() - startTime;


                /*
                KDefer defer = model.defer();
                model.createReusableTraversal().traverse(MetaDataSet.REL_COUNTRIES).then(defer.waitResult());
                model.createReusableTraversal().traverse(MetaDataSet.REL_USSTATES).then(defer.waitResult());
                model.createReusableTraversal().traverse(MetaDataSet.REL_STATIONS).then(defer.waitResult());


                final BufferedReader finalReader = reader;
                defer.then(new KCallback<Object[]>() {
                    public void on(Object[] objects) {

                        KObject[] countries = null;
                        KObject[] states = null;
                        KObject[] existingStations = null;
                        if (objects.length > 0 && objects[0] != null) {
                            countries = (KObject[]) objects[0];
                        }
                        if (objects.length > 1 && objects[1] != null) {
                            states = (KObject[]) objects[1];
                        }
                        if (objects.length > 2 && objects[2] != null) {
                            existingStations = (KObject[]) objects[2];
                        }




                    }
                });
                */

            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        } else {
            System.err.println("Country file not available locally !");
        }

        result.statistics.add(stats);
    }





    /*
    * ID            1-11   Character
    * LATITUDE     13-20   Real
    * LONGITUDE    22-30   Real
    * ELEVATION    32-37   Real
    * STATE        39-40   Character
    * NAME         42-71   Character
    * GSNFLAG      73-75   Character
    * HCNFLAG      77-79   Character
    * WMOID        81-85   Character
    * */

    private void processLine(final String line, final Stats stats) {

        final String id = line.substring(0, 11).trim();

        boolean stationExists = false;

        model.find(MetaStation.getInstance(), 0, KConfig.BEGINNING_OF_TIME, "id=" + id, new KCallback<KObject>() {
            public void on(KObject kObject) {
                if (kObject == null) {
                    //System.out.println("Line("+line.length()+"):" + line);
                    final String lat = line.substring(12, 20).trim();
                    final String lon = line.substring(21, 30).trim();
                    final String elv = line.substring(31, 37).trim();
                    final String state = line.substring(38, 40).trim();
                    final String name = line.substring(41, 71).trim();
                    final String gsnFlag = line.substring(72, 75).trim();
                    final String hcnFlag = line.substring(76, 79).trim();
                    final String wmoId = line.substring(80, line.length()).trim();


                    final Station station = model.createStation(0, KConfig.BEGINNING_OF_TIME)
                            .setId(id)
                            .setGsnFlag(!"".equals(gsnFlag))
                            .setHcnFlag(!"".equals(hcnFlag));

                    if (!"".equals(name)) {
                        station.setName(name);
                    }
                    if (!"".equals(lat)) {
                        station.setLatitude(Double.valueOf(lat));
                    }
                    if (!"".equals(lon)) {
                        station.setLongitude(Double.valueOf(lon));
                    }
                    if (!"".equals(elv)) {
                        station.setElevation(Double.valueOf(elv));
                    }

                    if (!"".equals(wmoId)) {
                        station.setWmoId(wmoId);
                    }
                    String localId = id.substring(0, 2);

                    KDefer defer = model.defer();
                    model.find(MetaCountry.getInstance(), 0, KConfig.BEGINNING_OF_TIME, "id=" + localId, defer.waitResult());
                    if (!"".equals(state)) {
                        model.find(MetaUSState.getInstance(), 0, KConfig.BEGINNING_OF_TIME, "id=" + state, defer.waitResult());
                    }

                    defer.then(new KCallback<Object[]>() {
                        public void on(Object[] objects) {
                            if (objects[0] != null) {
                                station.addCountry((Country) objects[0]);
                            }
                            if (objects[1] != null) {
                                station.addState((USState) objects[1]);
                            }
                        }
                    });

                    latch.countDown();
                    stats.insertions++;
                    if (stats.insertions != 0 && stats.insertions % 10000 == 0) {
                        System.out.println("Inserted " + stats.insertions + " so far.");
                         /*
                        model.save(new KCallback() {
                            public void on(Object o) {
                                if(o != null) {
                                    System.err.println(o.toString());
                                }
                            }
                        });
                        */
                    }
                } else {
                    System.out.println("Station exists.");
                    latch.countDown();
                }
            }
        });

    }
}

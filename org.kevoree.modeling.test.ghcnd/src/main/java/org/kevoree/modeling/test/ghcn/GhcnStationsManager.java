package org.kevoree.modeling.test.ghcn;

import kmf.ghcn.*;
import kmf.ghcn.impl.CountryImpl;
import kmf.ghcn.meta.MetaCountry;
import kmf.ghcn.meta.MetaDataSet;
import kmf.ghcn.meta.MetaStation;
import kmf.ghcn.meta.MetaUSState;
import org.kevoree.modeling.KCallback;
import org.kevoree.modeling.KObject;
import org.kevoree.modeling.defer.KDefer;
import org.kevoree.modeling.test.ghcn.utils.MyFtpClient;
import org.kevoree.modeling.test.ghcn.utils.Stats;

import java.io.*;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collection;

/**
 * Created by gregory.nain on 23/07/2014.
 */
public class GhcnStationsManager extends AbstractManager {

    protected static String serverAddress = "ftp.ncdc.noaa.gov";
    protected static String remoteDirectory = "/pub/data/ghcn/daily";
    protected static String localDirectory = "/tmp/ghcn";
    protected static String fileName = "ghcnd-stations.txt";

    public GhcnStationsManager(GhcndUniverse universe) {
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
                        final File countriesFile = ftp.getRemoteFile(remoteDirectory, fileName);
                        stats.time_download = System.currentTimeMillis() - startTime;
                        System.out.println("Downloading Complete:" + countriesFile.getAbsolutePath());


                        if (countriesFile != null && countriesFile.exists()) {

                            reader = new BufferedReader(new FileReader(countriesFile));
                            final ArrayList<String> lines = new ArrayList<String>();

                            KDefer defer = root.manager().model().defer();
                            root.traversal().traverse(MetaDataSet.REL_COUNTRIES).then(defer.waitResult());
                            root.traversal().traverse(MetaDataSet.REL_USSTATES).then(defer.waitResult());
                            root.traversal().traverse(MetaDataSet.REL_STATIONS).then(defer.waitResult());

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

                                    long startTime = System.currentTimeMillis();
                                    String line;
                                    try {
                                        while ((line = finalReader.readLine()) != null) {
                                            lines.add(line);
                                        }
                                    } catch (IOException e) {
                                        e.printStackTrace();
                                    }
                                    stats.time_readFile = System.currentTimeMillis() - startTime;


                                    startTime = System.currentTimeMillis();
                                    for (String l : lines) {
                                        //for(int i = 0; i < 35000; i++) {
                                        //processLine(lines.get(i), stats);
                                        processLine(l, stats, existingStations, countries, states);
                                        if (stats.insertions != 0 && stats.insertions % 10000 == 0) {
                                            System.out.println("Inserted " + stats.insertions + "/" + lines.size());
                                            root.manager().save(new KCallback<Throwable>() {
                                                public void on(Throwable throwable) {
                                                    if (throwable != null) {
                                                        throwable.printStackTrace();
                                                    }
                                                }
                                            });
                                        }
                                    }
                                    stats.time_insert = System.currentTimeMillis() - startTime;


                                    System.out.println("Stations Size:" + root.sizeOfStations());
                                    startTime = System.currentTimeMillis();
                                    root.manager().save(new KCallback<Throwable>() {
                                        public void on(Throwable throwable) {
                                            if (throwable != null) {
                                                throwable.printStackTrace();
                                            }
                                        }
                                    });
                                    stats.time_commit = System.currentTimeMillis() - startTime;


                                }
                            });
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

    private void processLine(final String line, final Stats stats, final KObject[] existingStations, final KObject[] countries, final KObject[] states) {

        final String id = line.substring(0, 11).trim();

        boolean stationExists = false;
        for (KObject station : existingStations) {
            if (station.get(MetaStation.ATT_ID).equals(id)) {
                stationExists = true;
                break;
            }
        }
        if (!stationExists) {
            //System.out.println("Line("+line.length()+"):" + line);
            final String lat = line.substring(12, 20).trim();
            final String lon = line.substring(21, 30).trim();
            final String elv = line.substring(31, 37).trim();
            final String state = line.substring(38, 40).trim();
            final String name = line.substring(41, 71).trim();
            final String gsnFlag = line.substring(72, 75).trim();
            final String hcnFlag = line.substring(76, 79).trim();
            final String wmoId = line.substring(80, line.length()).trim();


            final Station station = rootTimeView.createStation()
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
            for (KObject c : countries) {
                if (c.get(MetaCountry.ATT_ID).equals(localId)) {
                    station.addCountry((Country) c);
                    break;
                }
            }
            if (!"".equals(state)) {
                for (KObject s : states) {
                    if (s.get(MetaUSState.ATT_ID).equals(state)) {
                        station.addState((USState) s);
                        break;
                    }
                }
            }

            root.addStations(station);
            stats.insertions++;
        }

    }
}

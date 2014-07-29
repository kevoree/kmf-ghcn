package org.kevoree.modeling.test.ghcn;

import kmf.ghcn.*;
import kmf.ghcn.factory.GhcnFactory;
import org.kevoree.modeling.test.ghcn.utils.FtpClient;
import org.kevoree.modeling.test.ghcn.utils.Stats;

import java.io.*;
import java.util.ArrayList;

/**
 * Created by gregory.nain on 23/07/2014.
 */
public class GhcnStationsManager extends AbstractManager {

    protected static String serverAddress = "ftp.ncdc.noaa.gov";
    protected static String remoteDirectory = "/pub/data/ghcn/daily";
    protected static String localDirectory = "/tmp/ghcn";
    protected static String fileName = "ghcnd-stations.txt";

   public GhcnStationsManager(GhcnFactory factory) {
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
                        if( stats.insertions != 0 && stats.insertions % 10000 == 0){
                            System.out.println("Inserted " + stats.insertions + "/" + lines.size());
                        }
                    }
                    stats.time_insert = System.currentTimeMillis() - startTime;
                    startTime = System.currentTimeMillis();
                   // rootTimeView.commit();
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
    private void processLine(String line, Stats stats) {
        //System.out.println("Line("+line.length()+"):" + line);
        String id = line.substring(0,11).trim();
        String lat = line.substring(12,20).trim();
        String lon = line.substring(21,30).trim();
        String elv = line.substring(31,37).trim();
        String state = line.substring(38,40).trim();
        String name = line.substring(41,71).trim();
        String gsnFlag = line.substring(72,75).trim();
        String hcnFlag = line.substring(76,79).trim();
        String wmoId = line.substring(80,line.length()).trim();

        stats.lookups++;
        if(rootTimeView.lookup("/stations["+id+"]") == null) {
            Station station = rootTimeView.factory().createStation()
                    .withId(id)
                    .withGsnFlag(!"".equals(gsnFlag))
                    .withHcnFlag(!"".equals(hcnFlag));
            Country c = (Country)rootTimeView.lookup("/countries[" + id.substring(0,2) + "]");
            if(c != null) {
                station.setCountry(c);
            }
            if(!"".equals(name)) {
                station.setName(name);
            }
            if(!"".equals(lat)) {
                station.setLatitude(Float.valueOf(lat));
            }
            if(!"".equals(lon)) {
                station.setLongitude(Float.valueOf(lon));
            }
            if(!"".equals(elv)) {
                station.setElevation(Float.valueOf(elv));
            }
            if(!"".equals(state)) {
                USState s = (USState)rootTimeView.lookup("/usStates[" + state + "]");
                if(s != null) {
                    station.setState(s);
                }
            }
            if(!"".equals(wmoId)) {
                station.setWmoId(wmoId);
            }

            root.addStations(station);
            stats.insertions++;
        }
    }


}

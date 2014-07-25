package org.kevoree.modeling.test.ghcnd;

import kmf.ghcnd.*;
import org.kevoree.modeling.api.time.TimeView;
import org.kevoree.modeling.test.ghcnd.utils.FtpClient;

import java.io.*;
import java.util.ArrayList;

/**
 * Created by gregory.nain on 23/07/2014.
 */
public class GhcnStationsManager {

    protected TimeView<GhcndFactory> rootTimeView;
    //protected GhcndFactory factory;
    protected DataSet root;
    protected static String serverAddress = "ftp.ncdc.noaa.gov";
    protected static String remoteDirectory = "/pub/data/ghcn/daily";
    protected static String localDirectory = "/tmp/ghcn";
    protected static String fileName = "ghcnd-stations.txt";

    private int insertions = 0;
    private int lookups = 0;
    private long time_download = 0L;
    private long time_readFile = 0L;
    private long time_insert = 0L;
    private long time_commit = 0L;


    public GhcnStationsManager(GhcndFactory factory) {
        this.rootTimeView =  factory.time("0");
        //this.factory = factory;
        root = (DataSet)rootTimeView.lookup("/");
        if(root == null) {
            System.err.println("Could not reach the root");
        }

    }

    public void updateStations() {
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
                        if( insertions != 0 && insertions % 500 == 0){
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
    private void processLine(String line) {
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

        lookups++;
        if(rootTimeView.lookup("/stations["+id+"]") == null) {
            Station station = rootTimeView.factory().createStation()
        //if(factory.lookup("/stations["+id+"]") == null) {
         //   Station station = factory.createStation()
                    .withId(id)
                    .withGsnFlag(!"".equals(gsnFlag))
                    .withHcnFlag(!"".equals(hcnFlag));
            Country c = (Country)rootTimeView.lookup("/countries[" + id.substring(0,2) + "]");
            //Country c = (Country)factory.lookup("/countries[" + id.substring(0,2) + "]");
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
                //USState s = (USState)factory.lookup("/usStates[" + state + "]");
                if(s != null) {
                    station.setState(s);
                }
            }
            if(!"".equals(wmoId)) {
                station.setWmoId(wmoId);
            }

            root.addStations(station);
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

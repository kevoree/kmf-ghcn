package org.kevoree.modeling.test.ghcnd;

import kmf.ghcnd.*;
import org.kevoree.modeling.api.KMFContainer;
import org.kevoree.modeling.api.time.TimeView;
import org.kevoree.modeling.test.ghcnd.utils.FtpClient;

import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Created by gregory.nain on 23/07/2014.
 */
public class GhcnDailyManager {

    protected TimeView<GhcndFactory> rootTimeView;
    protected GhcndFactory factory;
    protected DataSet root;
    protected static String serverAddress = "ftp.ncdc.noaa.gov";
    protected static String remoteDirectory = "/pub/data/ghcn/daily/all";
    protected static String localDirectory = "/tmp/ghcn";
    protected static String fileName = "ghcnd-countries.txt";
    protected SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMd");

    private int insertions = 0;
    private int lookups = 0;
    private long time_readFileList = 0L;
    private long time_download = 0L;
    private long time_readFile = 0L;
    private long time_insert = 0L;
    private long time_commit = 0L;

    public GhcnDailyManager(GhcndFactory factory) {

        this.factory = factory;
        this.rootTimeView =  factory.time("0");

        root = (DataSet)rootTimeView.lookup("/");
        if(root == null) {
            System.err.println("Could not reach the root");
        }

    }

    public void updateDailyValues() {

        if(root != null) {
            FtpClient ftp = new FtpClient(serverAddress, remoteDirectory, localDirectory, null, null);
            root = (DataSet)rootTimeView.lookup("/");
            List<Station> stationList = root.getStations();
            if(stationList == null || stationList.size() == 0) {
                System.err.println("No station found!");
                return;
            }
            //for(String fileName : fileList) {
            for(int i = 0 ; i < 2 ; i++) {
                time_readFileList = 0L;
                time_download = 0L;
                time_readFile = 0L;
                time_insert = 0L;
                time_commit = 0L;
                String stationId = stationList.get(i).internalGetKey() + ".dly";
                processFile(ftp, stationId);
                System.out.println(toString() + " file:" + stationId);
                //processFile(ftp, fileName);
                //System.out.println(toString() + " file:" + fileName);
            }



        }

    }


    private void processFile(FtpClient ftp, String fileName) {
        try {
            System.out.println("Downloading :" + remoteDirectory + "/" + fileName);
            long startTime = System.currentTimeMillis();
            File measuresFile = ftp.getRemoteFile(remoteDirectory, fileName);
            time_download = System.currentTimeMillis() - startTime;
            System.out.println("Downloading Complete:" + measuresFile.getAbsolutePath());

            if (measuresFile != null && measuresFile.exists()) {
                BufferedReader reader = new BufferedReader(new FileReader(measuresFile));
                String line = null;
                ArrayList<String> lines = new ArrayList<String>();
                startTime = System.currentTimeMillis();
                while ((line = reader.readLine()) != null) {
                    lines.add(line);
                }
                time_readFile = System.currentTimeMillis() - startTime;
                startTime = System.currentTimeMillis();
                int lineNum = 0;
                for (String l : lines) {
                    processLine(l);
                    lineNum++;
                    if (insertions != 0 && insertions % 50 == 0) {
                        System.out.println("Processed "+lineNum + "/" + lines.size() +" Inserted " + insertions + "/" + 31*lines.size());
                    }
                }
                time_insert = System.currentTimeMillis() - startTime;

                startTime = System.currentTimeMillis();
                factory.commitAll();
                time_commit = System.currentTimeMillis() - startTime;
            } else {
                System.err.println("Measures file not available locally !");
            }
        } catch(IOException e) {
            e.printStackTrace();
        }
    }


    /*
ID            1-11   Character
YEAR         12-15   Integer
MONTH        16-17   Integer
ELEMENT      18-21   Character
VALUE1       22-26   Integer
MFLAG1       27-27   Character
QFLAG1       28-28   Character
SFLAG1       29-29   Character
VALUE2       30-34   Integer
MFLAG2       35-35   Character
QFLAG2       36-36   Character
SFLAG2       37-37   Character
  .           .          .
  .           .          .
  .           .          .
VALUE31    262-266   Integer
MFLAG31    267-267   Character
QFLAG31    268-268   Character
SFLAG31    269-269   Character
    * */
    private void processLine(String line) {
        try {
            String stationId = line.substring(0,11).trim();
            String year = line.substring(11, 15).trim();
            String month = line.substring(15,17).trim();
            String elementType = line.substring(17,21).trim();

            int baseIdx;
            for(int i = 1 ; i <= 31; i++) {
                baseIdx = 21 + (8*(i-1));
                String value = line.substring(baseIdx,baseIdx+5).trim();
                String mFlag = line.substring(baseIdx+5, baseIdx+6).trim();
                String qFlag = line.substring(baseIdx+6, baseIdx+7).trim();
                String sFlag = line.substring(baseIdx+7, baseIdx+8).trim();

                TimeView<GhcndFactory> timeView = factory.time(simpleDateFormat.parse(year + month + i).getTime() + "");
                Station station = (Station) timeView.lookup("/stations["+stationId+"]");
                if(station != null) {
                    Record record = station.findLastRecordsByID(elementType);
                    if(record == null) {
                        record = timeView.factory().createRecord().withType(elementType);
                        station.addLastRecords(record);
                    } else {
                        if(record.getNow() == timeView.now()) {
                            continue;
                        }
                    }
                    record.withValue(value).withQuality(qFlag).withSource(sFlag).withMeasurement(mFlag);
                    insertions++;
                    //timeView.commit();
                }
            }

        } catch (ParseException e) {
            e.printStackTrace();
        }
    }

    @Override
    public String toString() {
        return "[DailyManager] Stats:: Insertions:" + insertions + " Lookups:" + lookups
                + " ReadFileListTime:" + time_readFile + "ms"
                + " DownloadTime:" + time_download + "ms"
                + " ReadFileTime:" + time_readFile + "ms"
                + " InsertionTime:" + time_insert + "ms"
                + " CommitTime:" + time_commit + "ms";
    }
}
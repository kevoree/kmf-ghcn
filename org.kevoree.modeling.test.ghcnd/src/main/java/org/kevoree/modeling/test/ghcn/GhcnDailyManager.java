package org.kevoree.modeling.test.ghcn;

import kmf.ghcn.*;
import kmf.ghcn.factory.GhcnFactory;
import kmf.ghcn.factory.GhcnTimeView;
import kmf.ghcn.factory.GhcnTransaction;
import kmf.ghcn.factory.GhcnTransactionManager;
import org.kevoree.modeling.api.time.TimeView;
import org.kevoree.modeling.test.ghcn.utils.FtpClient;
import org.kevoree.modeling.test.ghcn.utils.Stats;

import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.TimeZone;

/**
 * Created by gregory.nain on 23/07/2014.
 */
public class GhcnDailyManager extends AbstractManager{

    protected static String serverAddress = "ftp.ncdc.noaa.gov";
    protected static String remoteDirectory = "/pub/data/ghcn/daily/all";
    protected static String localDirectory = "/tmp/ghcn";

    public GhcnDailyManager(GhcnTransactionManager tm) {
        super(tm);

    }

    public void run() {

        try {

            GhcnTransaction transaction = tm.createTransaction();
            this.rootTimeView = transaction.time(simpleDateFormat.parse("18000101").getTime());

            //root = (DataSet)baseFactory.lookup("/");
            root = (DataSet) rootTimeView.lookup("/");

            if (root != null) {
                FtpClient ftp = new FtpClient(serverAddress, remoteDirectory, localDirectory, null, null);
                List<Station> stationList = root.getStations();
                if (stationList == null || stationList.size() == 0) {
                    System.err.println("No station found!");
                    return;
                }

                for (int i = 0; i < 2; i++) {
                    String stationId = stationList.get(i).internalGetKey();
                    String stationFileName = stationId + ".dly";
                    Stats stats = new Stats(getClass().getSimpleName() + "::" + stationId);
                    processFile(ftp, stationFileName, stats, transaction);
                    result.statistics.add(stats);
                }

                transaction.close();
            }

        } catch (ParseException e) {
            e.printStackTrace();
        }
    }

    private void processFile(FtpClient ftp, String fileName, Stats stats, GhcnTransaction transaction) {
        try {
            System.out.println("Downloading :" + remoteDirectory + "/" + fileName);
            long startTime = System.currentTimeMillis();
            File measuresFile = ftp.getRemoteFile(remoteDirectory, fileName);
            stats.time_download = System.currentTimeMillis() - startTime;
            System.out.println("Downloading Complete:" + measuresFile.getAbsolutePath());

            if (measuresFile != null && measuresFile.exists()) {
                BufferedReader reader = new BufferedReader(new FileReader(measuresFile));
                String line = null;
                ArrayList<String> lines = new ArrayList<String>();
                startTime = System.currentTimeMillis();
                while ((line = reader.readLine()) != null) {
                    lines.add(line);
                }
                stats.time_readFile = System.currentTimeMillis() - startTime;
                startTime = System.currentTimeMillis();
                int lineNum = 0;
                for (String l : lines) {
                    processLine(l, stats, transaction);
                    lineNum++;
                    if (stats.insertions != 0 && stats.insertions % 50 == 0) {
                        System.out.println("Processed "+lineNum + "/" + lines.size() +" Inserted " + stats.insertions + "/" + 31*lines.size());
                    }
                }
                stats.time_insert = System.currentTimeMillis() - startTime;

                startTime = System.currentTimeMillis();
                transaction.commit();
                stats.time_commit = System.currentTimeMillis() - startTime;
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
    private void processLine(String line, Stats stats, GhcnTransaction transaction) {
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
                String date = year + month + i;
                //System.out.println("RawDate:" + date);
                //System.out.println("ParsedTime:" + simpleDateFormat.parse(date).toString() + ":" + simpleDateFormat.parse(date).getTime());
                if("-9999".equals(value) && "".equals(mFlag) && "".equals(qFlag) && "".equals(sFlag)) {
                    continue;
                }
                GhcnTimeView timeView = transaction.time(simpleDateFormat.parse(date).getTime());

                Station station = (Station) timeView.lookup("/stations["+stationId+"]");
                if(station != null) {
                    Record record = station.findLastRecordsByID(elementType);
                    if(record == null) {
                        record = timeView.createRecord().withType(elementType);
                        station.addLastRecords(record);
                    } else {
                        if(record.getNow() == timeView.now()) {
                            continue;
                        }

                    }
                    record.withValue(value).withQuality(qFlag).withSource(sFlag).withMeasurement(mFlag);
                    stats.insertions++;
                } else {
                    System.out.println("Station not found at time point:" + timeView.now());
                }
        /*
        */
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}

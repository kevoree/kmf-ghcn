package org.kevoree.modeling.test.ghcn;

import kmf.ghcn.*;
import kmf.ghcn.meta.MetaRecord;
import kmf.ghcn.meta.MetaStation;
import org.kevoree.modeling.KCallback;
import org.kevoree.modeling.KConfig;
import org.kevoree.modeling.KObject;
import org.kevoree.modeling.test.ghcn.utils.MyFtpClient;
import org.kevoree.modeling.test.ghcn.utils.Stats;

import java.io.*;
import java.util.ArrayList;

/**
 * Created by gregory.nain on 23/07/2014.
 */
public class GhcnDailyManager extends AbstractManager {

    protected static String serverAddress = "ftp.ncdc.noaa.gov";
    protected static String remoteDirectory = "/pub/data/ghcn/daily/all";
    protected static String localDirectory = "/tmp/ghcn";

    public GhcnDailyManager(GhcndModel model) {
        super(model);

    }

    public void run() {

        final Stats stats = new Stats(getClass().getSimpleName());

        MyFtpClient ftp = new MyFtpClient(serverAddress, remoteDirectory, localDirectory, null, null);
        final MyFtpClient finalFTP = ftp;
        model.findAll(MetaStation.getInstance(), 0, KConfig.BEGINNING_OF_TIME, new KCallback<KObject[]>() {
            public void on(KObject[] kObjects) {
                for (int i = 0; i < kObjects.length; i++) {
                    if (i < 2) {
                        Station station = (Station) kObjects[i];
                        String stationId = station.getId();
                        String stationFileName = stationId + ".dly";
                        processFile(finalFTP, stationFileName, stats);
                        result.statistics.add(stats);
                    }
                }
                model.save(null);
            }
        });

    }

    private void processFile(MyFtpClient ftp, String fileName, Stats stats) {
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
                    processLine(l, stats);
                    lineNum++;
                    if (stats.insertions != 0 && stats.insertions % 50 == 0) {
                        System.out.println("Processed " + lineNum + "/" + lines.size() + " Inserted " + stats.insertions + "/" + 31 * lines.size());
                    }
                }
                stats.time_insert = System.currentTimeMillis() - startTime;

                startTime = System.currentTimeMillis();
                model.save(null);
                stats.time_commit = System.currentTimeMillis() - startTime;
            } else {
                System.err.println("Measures file not available locally !");
            }
        } catch (IOException e) {
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
    private void processLine(final String line, final Stats stats) {
        try {
            final String stationId = line.substring(0, 11).trim();
            final String year = line.substring(11, 15).trim();
            final String month = line.substring(15, 17).trim();
            final String elementType = line.substring(17, 21).trim();

            int baseIdx;
            for (int i = 1; i <= 31; i++) {
                baseIdx = 21 + (8 * (i - 1));
                final String value = line.substring(baseIdx, baseIdx + 5).trim();
                final String mFlag = line.substring(baseIdx + 5, baseIdx + 6).trim();
                final String qFlag = line.substring(baseIdx + 6, baseIdx + 7).trim();
                final String sFlag = line.substring(baseIdx + 7, baseIdx + 8).trim();
                final String date = year + month + i;
                //System.out.println("RawDate:" + date);
                //System.out.println("ParsedTime:" + simpleDateFormat.parse(date).toString() + ":" + simpleDateFormat.parse(date).getTime());
                if ("-9999".equals(value) && "".equals(mFlag) && "".equals(qFlag) && "".equals(sFlag)) {
                    continue;
                }

                final long now = simpleDateFormat.parse(date).getTime();


                model.find(MetaStation.getInstance(), 0, now, "id=" + stationId, new KCallback<KObject>() {
                    public void on(KObject kObject) {
                        if (kObject != null) {
                            final Station station = (Station) kObject;
                            if (station != null) {
                                station.traversal().traverse(MetaStation.REL_RECORDS).withAttribute(MetaRecord.ATT_TYPE, elementType).then(new KCallback<KObject[]>() {
                                    public void on(KObject[] records) {
                                        if (records != null && records.length != 0) {
                                            Record record = (Record) records[0];
                                            record.setValue(value).setQuality(qFlag).setSource(sFlag).setMeasurement(mFlag);
                                            stats.insertions++;
                                        } else {
                                            Record record = model.createRecord(0, now).setType(elementType).setValue(value).setQuality(qFlag).setSource(sFlag).setMeasurement(mFlag);
                                            station.addRecords(record);

                                        }
                                    }
                                });
                            } else {
                                System.out.println("Station not found at time point:" + now);
                            }
                        }
                    }
                });

            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}

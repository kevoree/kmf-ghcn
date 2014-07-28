package org.kevoree.modeling.test.ghcn.utils;

import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.net.ftp.FTPReply;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Created by gregory.nain on 23/07/2014.
 */
public class FtpClient {

    private String serverAddress;
    private String remoteDirectory;
    private String localDirectory;
    private String userId;
    private String password;
    private boolean initiated = false;
    private FTPClient ftp;

    public FtpClient(String serverAddress, String remoteDirectory, String localDirectory, String userId, String password) {
        this.serverAddress = serverAddress;
        this.remoteDirectory = remoteDirectory;
        this.localDirectory = localDirectory;
        this.userId = userId;
        this.password = password;
    }


    private void initiate() {
        try {
            if(!initiated) {
                //new ftp client
                ftp = new FTPClient();
                //try to connect
                ftp.connect(serverAddress);
                //login to server
                ftp.enterLocalPassiveMode();

                if (userId != null && password != null) {
                    if (!ftp.login(userId, password)) {
                        ftp.logout();
                    }
                } else {
                    if(!ftp.login("anonymous","hey@hey.com")) {
                        ftp.logout();
                    }
                }
                int reply = ftp.getReplyCode();
                if (!FTPReply.isPositiveCompletion(reply)) {
                    ftp.disconnect();
                }

                ftp.setListHiddenFiles(true);
                ftp.setRemoteVerificationEnabled(false);

            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public void disconnect() {
        try {
            ftp.logout();
            ftp.disconnect();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public List<String> listDistantFilesFile(String remoteDirectory) {
        initiate();
        //change current directory
        ArrayList<String> files = null;
        try {
            ftp.changeWorkingDirectory(remoteDirectory);
            System.out.println("Current directory is " + ftp.printWorkingDirectory());
            System.out.println("Retrieving File list...");
            FTPFile[] ftpFiles = ftp.listFiles();
            System.out.println("Retrieved.");
            if (ftpFiles != null && ftpFiles.length > 0) {
                files = new ArrayList<String>();
                for (FTPFile file : ftpFiles) {
                    files.add(file.getName());
                }
            }
        }catch(IOException e){
            e.printStackTrace();
        }

        return files;
    }

    public File getRemoteFile(String remoteDirectory, String fileName) {
        initiate();
        File locDirFile = new File(localDirectory);
        if(!locDirFile.exists()) {
            locDirFile.mkdirs();
        }
        try {

            String remoteModificationTime = ftp.getModificationTime(remoteDirectory + "/" + fileName);
            String[] split = remoteModificationTime.split(" ");
            Date remoteDate = new SimpleDateFormat("yyyyMMddHHmmss").parse(split[1]);

            ftp.changeWorkingDirectory(remoteDirectory);
            File result = new File(localDirectory + "/" + fileName);

            boolean download = true;
            if(result.exists()) {
                long localLastModified = result.lastModified();
                Date localDate = new Date(localLastModified);
                if (localDate.getTime() == remoteDate.getTime()) {
                    //System.out.println("localLastModified:" + localDate + " remoteModificationTime:" + remoteDate);
                    download = false;
                }
            }

            if(download) {
                System.out.println("File update available. Downloading....");
                OutputStream output = new FileOutputStream(result);
                //get the file from the remote system
                ftp.retrieveFile(fileName, output);
                //close output stream
                output.close();
                result.setLastModified(remoteDate.getTime());
            } else {
                System.out.println("File already cached.");
            }

            return result;
        }catch(IOException e){
            e.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return null;
    }

/*
    public static void main(String[] args) {

        FtpClient ftp = new FtpClient("ftp.ncdc.noaa.gov","/pub/data/ghcn/daily","/tmp/ghcn","anonymous", "t@t.com");
        for(String f : ftp.listDistantFilesFile("/")) {
            System.out.println("->" + f);
        }
    }
*/


}

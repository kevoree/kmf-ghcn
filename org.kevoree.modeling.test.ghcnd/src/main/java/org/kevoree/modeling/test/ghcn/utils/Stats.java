package org.kevoree.modeling.test.ghcn.utils;

/**
 * Created by gregory.nain on 28/07/2014.
 */
public class Stats {
    String description;
    public int insertions = 0;
    public int lookups = 0;
    public long time_download = 0L;
    public long time_readFile = 0L;
    public long time_insert = 0L;
    public long time_commit = 0L;

    public Stats(String description) {
        this.description = description;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("[");
        sb.append(description);
        sb.append("] Insertions:");
        sb.append(insertions);
        sb.append(" Lookups:");
        sb.append(lookups);
        sb.append(" DownloadTime:");
        sb.append(time_download);
        sb.append(" ReadFileTime:");
        sb.append(time_readFile);
        sb.append(" InsertionTime:");
        sb.append(time_insert);
        sb.append(" CommitTime:");
        sb.append(time_commit);
        return sb.toString();
    }
}

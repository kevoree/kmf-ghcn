package org.kevoree.modeling.test.ghcn;

import kmf.ghcn.DataSet;
import kmf.ghcn.GhcndUniverse;
import kmf.ghcn.GhcndView;
import org.kevoree.modeling.test.ghcn.utils.UpdateResult;

import java.text.SimpleDateFormat;
import java.util.TimeZone;

/**
 * Created by gregory.nain on 28/07/2014.
 */
public abstract class AbstractManager implements Runnable{

    protected GhcndView rootTimeView;
    protected GhcndUniverse universe;
    protected DataSet root;
    protected UpdateResult result;
    protected SimpleDateFormat simpleDateFormat;

    public AbstractManager(GhcndUniverse universe) {
        this.universe = universe;
        simpleDateFormat = new SimpleDateFormat("yyyyMMd");
        simpleDateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
        result = new UpdateResult();
    }

    public UpdateResult getResult() {
        return result;
    }

}

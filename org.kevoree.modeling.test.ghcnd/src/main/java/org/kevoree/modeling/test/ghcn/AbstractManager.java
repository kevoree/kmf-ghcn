package org.kevoree.modeling.test.ghcn;

import kmf.ghcn.DataSet;
import kmf.ghcn.factory.GhcnFactory;
import kmf.ghcn.factory.GhcnTimeView;
import kmf.ghcn.factory.GhcnTransactionManager;
import org.kevoree.modeling.api.time.TimeView;
import org.kevoree.modeling.test.ghcn.utils.UpdateResult;

import java.text.SimpleDateFormat;
import java.util.TimeZone;

/**
 * Created by gregory.nain on 28/07/2014.
 */
public abstract class AbstractManager implements Runnable{

    protected GhcnTimeView rootTimeView;
    protected GhcnTransactionManager tm;
    protected DataSet root;
    protected UpdateResult result;
    protected SimpleDateFormat simpleDateFormat;

    public AbstractManager(GhcnTransactionManager tm) {
        this.tm = tm;
        simpleDateFormat = new SimpleDateFormat("yyyyMMd");
        simpleDateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
        //this.rootTimeView =  factory.time("0");
        //root = (DataSet)rootTimeView.lookup("/");

        result = new UpdateResult();
    }

    public UpdateResult getResult() {
        return result;
    }

}

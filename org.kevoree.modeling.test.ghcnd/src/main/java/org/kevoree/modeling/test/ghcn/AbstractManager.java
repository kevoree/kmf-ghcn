package org.kevoree.modeling.test.ghcn;

import kmf.ghcn.GhcndModel;
import org.kevoree.modeling.test.ghcn.utils.UpdateResult;

import java.text.SimpleDateFormat;
import java.util.TimeZone;

/**
 * Created by gregory.nain on 28/07/2014.
 */
public abstract class AbstractManager implements Runnable{

    protected GhcndModel model;
    protected UpdateResult result;
    protected SimpleDateFormat simpleDateFormat;

    public AbstractManager(GhcndModel model) {
        this.model = model;
        simpleDateFormat = new SimpleDateFormat("yyyyMMd");
        simpleDateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
        result = new UpdateResult();
    }

    public UpdateResult getResult() {
        return result;
    }

}

package org.kevoree.modeling.test.ghcn;

import kmf.ghcn.DataSet;
import kmf.ghcn.factory.GhcnFactory;
import org.kevoree.modeling.api.time.TimeView;
import org.kevoree.modeling.test.ghcn.utils.UpdateResult;

/**
 * Created by gregory.nain on 28/07/2014.
 */
public abstract class AbstractManager implements Runnable{

    protected TimeView<GhcnFactory> rootTimeView;
    protected GhcnFactory baseFactory;
    protected DataSet root;
    protected UpdateResult result;

    public AbstractManager(GhcnFactory factory) {
        this.baseFactory = factory;
        this.rootTimeView =  factory.time("0");
        root = (DataSet)rootTimeView.lookup("/");
        if(root == null) {
            System.err.println("Could not reach the root");
        }
        result = new UpdateResult();
    }

    public UpdateResult getResult() {
        return result;
    }

}

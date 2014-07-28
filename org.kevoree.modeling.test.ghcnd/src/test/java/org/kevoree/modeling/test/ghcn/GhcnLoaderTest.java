package org.kevoree.modeling.test.ghcn;

import org.junit.Test;


/**
 * Created by gregory.nain on 23/07/2014.
 */
public class GhcnLoaderTest {


    @Test
    public void mainTest() {

        GhcnLoader ghcn = new GhcnLoader();
        ghcn.initFactory();
        //ghcn.updateCountries();
        //ghcn.updateUSStates();
        //ghcn.updateStations();
        //ghcn.updateDaily();
        ghcn.updateAll();
        ghcn.waitCompletion();

    }


}

package org.kevoree.modeling.test.ghcnd;

import org.junit.Test;

import java.io.File;

/**
 * Created by gregory.nain on 23/07/2014.
 */
public class GhcndLoaderTest {


    @Test
    public void mainTest() {

        GhcndLoader ghcnd = new GhcndLoader();
        ghcnd.initFactory();
        //ghcnd.updateCountries();
        //ghcnd.updateUSStates();
        //ghcnd.updateStations();
        ghcnd.updateDaily();

    }


}

package org.kevoree.modeling.test.ghcn;

import org.junit.Test;
import org.kevoree.modeling.KCallback;

import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;


/**
 * Created by gregory.nain on 23/07/2014.
 */
public class GhcnLoaderTest {


    private void deleteBase() throws IOException {
        Path directory = Paths.get("GhcnLevelDB");
        if (directory.toFile().exists()) {
            Files.walkFileTree(directory, new SimpleFileVisitor<Path>() {
                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                    Files.delete(file);
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                    Files.delete(dir);
                    return FileVisitResult.CONTINUE;
                }

            });
        }
    }

    //@Test
    public void mainTest() throws IOException {

        deleteBase();

        final GhcnLoader ghcn = new GhcnLoader();
        ghcn.initFactory(new KCallback() {
            public void on(Object o) {
                ghcn.updateCountries();
                ghcn.updateUSStates();
                ghcn.updateStations();
                //ghcn.updateDaily();

                ghcn.waitCompletion();
                ghcn.free();

            }
        });

/*
        GhcnReader reader = new GhcnReader();
        reader.printDates();
*/
        //reader.printCountries();
        //reader.printStates();
        //reader.printStations();

    }

    public static void main(String[] args) {
        try {

            GhcnLoaderTest test = new GhcnLoaderTest();
            test.mainTest();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }


}

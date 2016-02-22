package org.kevoree.modeling.test.ghcn.utils;

import org.kevoree.modeling.test.ghcn.AbstractManager;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Created by gregory.nain on 28/07/2014.
 */
public class ThreadPoolManager {

    private static ExecutorService threadPool = Executors.newSingleThreadExecutor();//Executors.newFixedThreadPool(1);

    public static Queue<Future<UpdateResult>> waitingExecution = new ConcurrentLinkedQueue<Future<UpdateResult>>();

    public static Future<UpdateResult> addTask(AbstractManager run) {
        Future<UpdateResult> f = threadPool.submit(run, run.getResult());
        waitingExecution.add(f);
        return f;
    }
    public static void shutdown() {
        threadPool.shutdown();
    }

}

package org.kevoree.modeling.test.ghcn;


/**
 * Created by gregory.nain on 31/07/2014.
 */
public class WalkerTest {
/*
    private static String dbLocation = "GhcnLevelDB";
    private static GhcnTransactionManager tm;
    private static SimpleDateFormat simpleDateFormat;
    private static TimeMeta timeMetaRoot;


    @BeforeClass
    public static void init() {
        try {
            tm = new GhcnTransactionManager(new LevelDbDataStore(dbLocation));

            simpleDateFormat = new SimpleDateFormat("yyyyMMd");
            simpleDateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));

            GhcnTransaction transaction = tm.createTransaction();
            GhcnTimeView rootTimeView = transaction.time(simpleDateFormat.parse("18000101").getTime());
            DataSet root = (DataSet)rootTimeView.lookup("/");
            if(root == null) {
                System.err.println("Root not found !");
                System.exit(-1);
            }

            timeMetaRoot = ((TimeAwareKMFFactory)rootTimeView).getTimeTree("#global");

        } catch (ParseException e) {
            e.printStackTrace();
        }
    }

    @AfterClass
    public static void close() {
        tm.close();
    }

    public class AscendingWalker implements TimeWalker {

        public long origin, previous = 0;

        public AscendingWalker(long origin) {
            this.origin = origin;
        }

        @Override
        public void walk(@JetValueParameter(name = "timePoint") @NotNull long timePoint) {
            //System.out.println("" + SimpleDateFormat.getDateInstance().format(new Date(timePoint)));
            if(previous != 0) {
                assertTrue(previous <= timePoint);
            } else {
                assertTrue("TP:" + timePoint + " != " + origin, timePoint == origin);
            }
            previous = timePoint;
        }
    }

    @Test
    public void ascendingWalkerTest() {
        try {

            long origin = simpleDateFormat.parse("18000101").getTime();
            AscendingWalker walker = new AscendingWalker(origin);
            long latest = timeMetaRoot.getVersionTree().last().getKey();
            timeMetaRoot.walkAsc(walker);
            assert(walker.previous == latest);

        } catch (ParseException e) {
            e.printStackTrace();
        }
    }




    public class DescendingWalker implements TimeWalker {

        public long origin, previous = 0;

        public DescendingWalker(long origin) {
            this.origin = origin;
        }

        @Override
        public void walk(@JetValueParameter(name = "timePoint") @NotNull long timePoint) {
            //System.out.println("" + SimpleDateFormat.getDateInstance().format(new Date(timePoint)));
            if(previous != 0) {
                assertTrue(previous >= timePoint);
            } else {
                assertTrue("TP:" + timePoint + " != " + origin,timePoint == origin);
            }
            previous = timePoint;
        }
    }

    @Test
    public void descendingWalkerTest() {
        try {
            long origin = simpleDateFormat.parse("19181231").getTime();
            long first = simpleDateFormat.parse("18000101").getTime();
            DescendingWalker walker = new DescendingWalker(origin);
            timeMetaRoot.walkDesc(walker);
            assert(walker.previous == first);

        } catch (ParseException e) {
            e.printStackTrace();
        }
    }




    public class RangeAscendingWalker implements TimeWalker {

        public long origin, from, to, previous = 0;
        SimpleDateFormat localDateFormat;

        public RangeAscendingWalker(long origin, long from, long to) {
            this.origin = origin;
            this.from = from;
            this.to = to;

            localDateFormat = new SimpleDateFormat("yyyy.MM.d H:m:s");
            localDateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
        }

        @Override
        public void walk(@JetValueParameter(name = "timePoint") @NotNull long timePoint) {
            //System.out.println("" + SimpleDateFormat.getDateInstance().format(new Date(timePoint)));

            if(previous == 0) {
                boolean valid = (timePoint == origin);
                valid |= (timeMetaRoot.getVersionTree().previous(origin).getKey() == timePoint);
                valid |= (timeMetaRoot.getVersionTree().next(origin).getKey() == timePoint);
                assertTrue("Origin:"+localDateFormat.format(new Date(origin))
                        + " FirstTP:" + localDateFormat.format(new Date(timePoint))
                        + " Upper:" + localDateFormat.format(new Date(timeMetaRoot.getVersionTree().previous(origin).getKey()))
                        + " Lower:" + localDateFormat.format(new Date(timeMetaRoot.getVersionTree().next(origin).getKey())), valid);
            } else {
                assert(previous <= timePoint);
            }
            previous = timePoint;
        }
    }

    @Test
    public void rangeAscendingWalkerTest() {
        try {
            SimpleDateFormat localDateFormat = new SimpleDateFormat("yyyy.MM.d H:m:s");
            localDateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));

            long origin = localDateFormat.parse("1889.01.25 00:00:00").getTime();
            long start = localDateFormat.parse("1889.01.25 11:50:00").getTime();//25 janv. 1899 11h50
            long end = localDateFormat.parse("1889.02.04 11:50:00").getTime();//11 nov. 1909 11h50
            long tail = localDateFormat.parse("1889.02.04 00:00:00").getTime();//11 nov. 1909 11h50
            //System.out.println("Origin:"+localDateFormat.format(new Date(origin)) + " start:" + localDateFormat.format(new Date(start)));
            RangeAscendingWalker walker = new RangeAscendingWalker(origin, start, end);
            timeMetaRoot.walkRangeAsc(walker, start, end);
            assertTrue("Previous:"+localDateFormat.format(new Date(walker.previous)) + " tail:" + localDateFormat.format(new Date(tail)), walker.previous == tail);

        } catch (ParseException e) {
            e.printStackTrace();
        }
    }



    public class RangeDescendingWalker implements TimeWalker {

        public long origin, from, to, previous = 0;
        SimpleDateFormat localDateFormat;

        public RangeDescendingWalker(long origin, long from, long to) {
            this.origin = origin;
            this.from = from;
            this.to = to;
            localDateFormat = new SimpleDateFormat("yyyy.MM.d H:m:s");
            localDateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
        }

        @Override
        public void walk(@JetValueParameter(name = "timePoint") @NotNull long timePoint) {
            //System.out.println("" + SimpleDateFormat.getDateInstance().format(new Date(timePoint)));
            if(previous == 0) {
                boolean valid = (timePoint == origin);
                valid |= (timeMetaRoot.getVersionTree().previous(origin).getKey() == timePoint);
                valid |= (timeMetaRoot.getVersionTree().next(origin).getKey() == timePoint);
                assertTrue("Origin:"+localDateFormat.format(new Date(origin))
                        + " FirstTP:" + localDateFormat.format(new Date(timePoint))
                        + " Upper:" + localDateFormat.format(new Date(timeMetaRoot.getVersionTree().previous(origin).getKey()))
                        + " Lower:" + localDateFormat.format(new Date(timeMetaRoot.getVersionTree().next(origin).getKey())), valid);
            } else {
                assert(previous > timePoint);
            }
            previous = timePoint;
        }
    }
    @Test
    public void rangeDescendingWalkerTest() {
        try {

            SimpleDateFormat localDateFormat = new SimpleDateFormat("yyyy.MM.d H:m:s");
            localDateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));

            long origin = localDateFormat.parse("1889.02.04 00:00:00").getTime();//11 nov. 1909 11h50
            long start = localDateFormat.parse("1889.02.04 11:50:00").getTime();//11 nov. 1909 11h50
            long end = localDateFormat.parse("1889.01.25 11:50:00").getTime();//25 janv. 1899 11h50
            long tail = localDateFormat.parse("1889.01.25 00:00:00").getTime();

            RangeDescendingWalker walker = new RangeDescendingWalker(origin, start, end);
            timeMetaRoot.walkRangeDesc(walker, start, end);
            assertTrue("End:" + localDateFormat.format(new Date(end)) + " Last:" + localDateFormat.format(new Date(walker.previous)) + " Tail:" + localDateFormat.format(new Date(tail)), walker.previous == tail);

        } catch (ParseException e) {
            e.printStackTrace();
        }
    }



    @Test
    public void nextFunctionTest() {
        try {
            final GhcnTransaction testTransaction = tm.createTransaction();
            GhcnTimeView timeView = testTransaction.time(simpleDateFormat.parse("19890101").getTime());
            Record firstRecord = (Record) timeView.lookup("/stations[ASN00075156]/lastRecords[PRCP]");

            TimeMeta recordTimes = ((TimeAwareKMFFactory)timeView).getTimeTree(firstRecord.path());
            recordTimes.walkAsc(new TimeWalker() {
                long previousDotNow;
                TimeAwareKMFContainer previousDotNext;
                boolean firstPassed = false;

                @Override
                public void walk(@JetValueParameter(name = "timePoint") long l) {
                    //System.out.println("L:" + simpleDateFormat.format(new Date(l)));
                    GhcnTimeView myTimeView = testTransaction.time(l);
                    Record record = (Record) myTimeView.lookup("/stations[ASN00075156]/lastRecords[PRCP]");
                    if(firstPassed) {
                        assertTrue("Previous.next:" + simpleDateFormat.format(new Date(previousDotNext.getNow())) + " Previous.now:" + simpleDateFormat.format(new Date(previousDotNow)), previousDotNext.getNow() == l);
                    } else {
                        firstPassed = true;
                    }
                    previousDotNow = record.getNow();
                    previousDotNext = record.next();
                }
            });


        } catch (ParseException e) {
            e.printStackTrace();
        }
    }
    */


}


package simpledb;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;

/**
 * TableStats represents statistics (e.g., histograms) about base tables in a
 * query. 
 * 
 * This class is not needed in implementing proj1 and proj2.
 */
public class TableStats {

    private static final ConcurrentHashMap<String, TableStats> statsMap = new ConcurrentHashMap<String, TableStats>();

    static final int IOCOSTPERPAGE = 1000;

    public static TableStats getTableStats(String tablename) {
        return statsMap.get(tablename);
    }

    public static void setTableStats(String tablename, TableStats stats) {
        statsMap.put(tablename, stats);
    }
    
    public static void setStatsMap(HashMap<String,TableStats> s)
    {
        try {
            java.lang.reflect.Field statsMapF = TableStats.class.getDeclaredField("statsMap");
            statsMapF.setAccessible(true);
            statsMapF.set(null, s);
        } catch (NoSuchFieldException e) {
            e.printStackTrace();
        } catch (SecurityException e) {
            e.printStackTrace();
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }

    }

    public static Map<String, TableStats> getStatsMap() {
        return statsMap;
    }

    public static void computeStatistics() {
        Iterator<Integer> tableIt = Database.getCatalog().tableIdIterator();

        System.out.println("Computing table stats.");
        while (tableIt.hasNext()) {
            int tableid = tableIt.next();
            TableStats s = new TableStats(tableid, IOCOSTPERPAGE);
            setTableStats(Database.getCatalog().getTableName(tableid), s);
        }
        System.out.println("Done.");
    }

    /**
     * Number of bins for the histogram. Feel free to increase this value over
     * 100, though our tests assume that you have at least 100 bins in your
     * histograms.
     */
    static final int NUM_HIST_BINS = 100;

    // every element is either IntHistogram or StringHistogram
    // based on corresponding field type.
    private Object[] histograms;
    private HeapFile df;
    private int totalTups = 0;
    private int ioCostPerPage;
    
    /**
     * Create a new TableStats object, that keeps track of statistics on each
     * column of a table
     * 
     * @param tableid
     *            The table over which to compute statistics
     * @param ioCostPerPage
     *            The cost per page of IO. This doesn't differentiate between
     *            sequential-scan IO and disk seeks.
     */
    public TableStats(int tableid, int ioCostPerPage) {
        this.ioCostPerPage = ioCostPerPage;
        df = (HeapFile) Database.getCatalog().getDbFile(tableid);
        TupleDesc td = df.getTupleDesc();
        
        histograms = new Object[td.numFields()];
        Integer[] min = new Integer[td.numFields()];
        Integer[] max = new Integer[td.numFields()];
        for (int i = 0; i < td.numFields(); i++) {
            if (td.getFieldType(i) == Type.INT_TYPE) {
                min[i] = Integer.MAX_VALUE;
                max[i] = Integer.MIN_VALUE;
            }
        }
        
        DbFileIterator itr = df.iterator(new TransactionId());
        try {
            itr.open();
            while (itr.hasNext()) {
                Tuple t = itr.next();
                for (int i = 0; i < td.numFields(); i++) {
                    if (td.getFieldType(i) == Type.INT_TYPE) {
                        min[i] = Math.min(min[i],
                                ((IntField) t.getField(i)).getValue());
                        max[i] = Math.max(max[i],
                                ((IntField) t.getField(i)).getValue());
                    }
                }
            }
            
            for (int i = 0; i < td.numFields(); i++) {
                if (td.getFieldType(i) == Type.INT_TYPE) {
                    histograms[i] = new IntHistogram(TableStats.NUM_HIST_BINS,
                            min[i], max[i]);
                } else if (td.getFieldType(i) == Type.STRING_TYPE) {
                    histograms[i] = new StringHistogram(
                            TableStats.NUM_HIST_BINS);
                }
            }
            
            itr.rewind();
            while (itr.hasNext()) {
                totalTups++;
                Tuple t = itr.next();
                for (int i = 0; i < td.numFields(); i++) {
                    if (td.getFieldType(i) == Type.INT_TYPE) {
                        ((IntHistogram) histograms[i]).addValue(((IntField) t
                                .getField(i)).getValue());
                    } else if (td.getFieldType(i) == Type.STRING_TYPE) {
                        ((StringHistogram) histograms[i])
                                .addValue(((StringField) t.getField(i))
                                        .getValue());
                    }
                }
            }
            itr.close();
        } catch (NoSuchElementException e) {
            e.printStackTrace();
            System.exit(-1);
        } catch (DbException e) {
            e.printStackTrace();
            System.exit(-1);
        } catch (TransactionAbortedException e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }

    /**
     * Estimates the cost of sequentially scanning the file, given that the cost
     * to read a page is costPerPageIO. You can assume that there are no seeks
     * and that no pages are in the buffer pool.
     * 
     * Also, assume that your hard drive can only read entire pages at once, so
     * if the last page of the table only has one tuple on it, it's just as
     * expensive to read as a full page. (Most real hard drives can't
     * efficiently address regions smaller than a page at a time.)
     * 
     * @return The estimated cost of scanning the table.
     */
    public double estimateScanCost() {
        return ioCostPerPage * df.numPages();
    }

    /**
     * This method returns the number of tuples in the relation, given that a
     * predicate with selectivity selectivityFactor is applied.
     * 
     * @param selectivityFactor
     *            The selectivity of any predicates over the table
     * @return The estimated cardinality of the scan with the specified
     *         selectivityFactor
     */
    public int estimateTableCardinality(double selectivityFactor) {
        return (int) (selectivityFactor * totalTuples());
    }

    /**
     * The average selectivity of the field under op.
     * @param field
     *        the index of the field
     * @param op
     *        the operator in the predicate
     * The semantic of the method is that, given the table, and then given a
     * tuple, of which we do not know the value of the field, return the
     * expected selectivity. You may estimate this value from the histograms.
     * */
    public double avgSelectivity(int field, Predicate.Op op) {
        // some code goes here
        return 1.0;
    }

    /**
     * Estimate the selectivity of predicate <tt>field op constant</tt> on the
     * table.
     * 
     * @param field
     *            The field over which the predicate ranges
     * @param op
     *            The logical operation in the predicate
     * @param constant
     *            The value against which the field is compared
     * @return The estimated selectivity (fraction of tuples that satisfy) the
     *         predicate
     */
    public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
        if (histograms[field] instanceof IntHistogram) {
            return ((IntHistogram) histograms[field])
                    .estimateSelectivity(op, ((IntField) constant).getValue());
        } else if (histograms[field] instanceof StringHistogram) {
            return ((StringHistogram) histograms[field])
                    .estimateSelectivity(op, ((StringField) constant).getValue());
        }
        // Should never reach here
        return 1.0;
    }

    /**
     * return the total number of tuples in this table
     * */
    public int totalTuples() {
        return totalTups;
    }

}

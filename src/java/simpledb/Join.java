package simpledb;

import java.util.*;

/**
 * The Join operator implements the relational join operation.
 */
public class Join extends Operator {

    private static final long serialVersionUID = 1L;
    
    private JoinPredicate pred;
    private DbIterator child1, child2;
    private TupleDesc td, td1, td2;
    private ArrayList<Tuple> tupList;
    private Iterator<Tuple> itr;
    
    /**
     * Constructor. Accepts to children to join and the predicate to join them
     * on
     * 
     * @param p
     *            The predicate to use to join the children
     * @param child1
     *            Iterator for the left(outer) relation to join
     * @param child2
     *            Iterator for the right(inner) relation to join
     */
    public Join(JoinPredicate p, DbIterator child1, DbIterator child2) {
        this.pred = p;
        this.child1 = child1;
        this.child2 = child2;
        this.td1 = child1.getTupleDesc();
        this.td2 = child2.getTupleDesc();
        this.td = TupleDesc.merge(td1, td2);
        this.tupList = new ArrayList<Tuple>();
        this.itr = null;
    }

    public JoinPredicate getJoinPredicate() {
        return pred;
    }

    /**
     * @return
     *       the field name of join field1. Should be quantified by
     *       alias or table name.
     * */
    public String getJoinField1Name() {
        return td1.getFieldName(pred.getField1());
    }

    /**
     * @return
     *       the field name of join field2. Should be quantified by
     *       alias or table name.
     * */
    public String getJoinField2Name() {
        return td2.getFieldName(pred.getField2());
    }

    /**
     * @see simpledb.TupleDesc#merge(TupleDesc, TupleDesc) for possible
     *      implementation logic.
     */
    public TupleDesc getTupleDesc() {
        return td;
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        child1.open();
        child2.open();
        
        // Sort-Merge Join for equality join
        if (pred.getOperator() == Predicate.Op.EQUALS) {
            ArrayList<Tuple> child1Tups = new ArrayList<Tuple>();
            ArrayList<Tuple> child2Tups = new ArrayList<Tuple>();
            while (child1.hasNext()) {
                child1Tups.add(child1.next());
            }
            while (child2.hasNext()) {
                child2Tups.add(child2.next());
            }
            Collections.sort(child1Tups, new Comparator<Tuple>() {
                @Override
                public int compare(Tuple t1, Tuple t2) {
                    if (t1.getField(pred.getField1()).compare(
                            Predicate.Op.LESS_THAN,
                            t2.getField(pred.getField1()))) {
                        return -1;
                    } else if (t1.getField(pred.getField1()).compare(
                            Predicate.Op.EQUALS, t2.getField(pred.getField1()))) {
                        return 0;
                    } else {
                        return 1;
                    }
                }                
            });
            Collections.sort(child2Tups, new Comparator<Tuple>() {
                @Override
                public int compare(Tuple t1, Tuple t2) {
                    if (t1.getField(pred.getField2()).compare(
                            Predicate.Op.LESS_THAN,
                            t2.getField(pred.getField2()))) {
                        return -1;
                    } else if (t1.getField(pred.getField2()).compare(
                            Predicate.Op.EQUALS, t2.getField(pred.getField2()))) {
                        return 0;
                    } else {
                        return 1;
                    }
                }
            });
            
            int i = 0;
            int j = 0;
            while (i < child1Tups.size() && j < child2Tups.size()) {
                Field f1 = child1Tups.get(i).getField(pred.getField1());
                Field f2 = child2Tups.get(j).getField(pred.getField2());
                if (f1.compare(Predicate.Op.LESS_THAN, f2)) {
                    i++;
                } else if (f1.compare(Predicate.Op.GREATER_THAN, f2)) {
                    j++;
                } else {
                    int backupJ = j;
                    while (j < child2Tups.size()
                            && child2Tups.get(j).getField(pred.getField2())
                                    .compare(Predicate.Op.EQUALS, f2)) {
                        Tuple newTuple = new Tuple(td);
                        for (int k = 0; k < td.numFields(); k++) {
                            if (k < td1.numFields()) {
                                newTuple.setField(k, child1Tups.get(i).getField(k));
                            } else {
                                newTuple.setField(k, child2Tups.get(j).getField(k - td1.numFields()));
                            }
                        }
                        tupList.add(newTuple);
                        j++;
                    }
                    i++;
                    j = backupJ;
                }
            }
        } else {
            for ( ;child1.hasNext(); ) {
                Tuple t1 = child1.next();
                for ( ;child2.hasNext(); ) {
                    Tuple t2 = child2.next();
                    if (pred.filter(t1, t2)) {
                        Tuple newTuple = new Tuple(td);
                        for (int i = 0; i < td.numFields(); i++) {
                            if (i < td1.numFields()) {
                                newTuple.setField(i, t1.getField(i));
                            } else {
                                newTuple.setField(i, t2.getField(i - td1.numFields()));
                            }
                        }
                        tupList.add(newTuple);
                    }
                }
                child2.rewind();
            }
        }
        itr = tupList.iterator();
        super.open();
    }

    public void close() {
        super.close();
        itr = null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        itr = tupList.iterator();
    }

    /**
     * Returns the next tuple generated by the join, or null if there are no
     * more tuples. Logically, this is the next tuple in r1 cross r2 that
     * satisfies the join predicate. There are many possible implementations;
     * the simplest is a nested loops join.
     * <p>
     * Note that the tuples returned from this particular implementation of Join
     * are simply the concatenation of joining tuples from the left and right
     * relation. Therefore, if an equality predicate is used there will be two
     * copies of the join attribute in the results. (Removing such duplicate
     * columns can be done with an additional projection operator if needed.)
     * <p>
     * For example, if one tuple is {1,2,3} and the other tuple is {1,5,6},
     * joined on equality of the first column, then this returns {1,2,3,1,5,6}.
     * 
     * @return The next matching tuple.
     * @see JoinPredicate#filter
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if (itr != null && itr.hasNext()) {
            return itr.next();
        } else {
            return null;
        }
        /*if (t1 == null) {
            if (child1.hasNext()) {
                t1 = child1.next();
            }
        }
        
        while (t1 != null) {
            while (child2.hasNext()) {
                Tuple t2 = child2.next();
                if (pred.filter(t1, t2)) {
                    Tuple newTuple = new Tuple(td);
                    for (int i = 0; i < td.numFields(); i++) {
                        if (i < td1.numFields()) {
                            newTuple.setField(i, t1.getField(i));
                        } else {
                            newTuple.setField(i, t2.getField(i - td1.numFields()));
                        }
                    }
                    return newTuple;
                }
            }
            
            if (child1.hasNext()) {
                t1 = child1.next();
                child2.rewind();
            } else {
                t1 = null;
            }
        }
        
        return null;*/
    }

    @Override
    public DbIterator[] getChildren() {
        return new DbIterator[] {child1, child2};
    }

    @Override
    public void setChildren(DbIterator[] children) {
        child1 = children[0];
        child2 = children[1];
    }

}

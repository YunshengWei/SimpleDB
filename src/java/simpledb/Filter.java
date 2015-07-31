package simpledb;

import java.util.*;

/**
 * Filter is an operator that implements a relational select.
 */
public class Filter extends Operator {

    private static final long serialVersionUID = 1L;
    
    private Predicate pred;
    private DbIterator child;
    private TupleDesc td;
    private ArrayList<Tuple> tupList;
    private Iterator<Tuple> itr;

    /**
     * Constructor accepts a predicate to apply and a child operator to read
     * tuples to filter from.
     * 
     * @param p
     *            The predicate to filter tuples with
     * @param child
     *            The child operator
     */
    public Filter(Predicate p, DbIterator child) {
        this.pred = p;
        this.child = child;
        this.td = child.getTupleDesc();
        this.tupList = new ArrayList<Tuple>();
        this.itr = null;
    }

    public Predicate getPredicate() {
        return pred;
    }

    public TupleDesc getTupleDesc() {
        return td;
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        child.open();
        // preload all valid tuples in a list
        // can be efficient for rewind()
        while (child.hasNext()) {
            Tuple t = child.next();
            if (pred.filter(t)) {
                tupList.add(t);
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
     * AbstractDbIterator.readNext implementation. Iterates over tuples from the
     * child operator, applying the predicate to them and returning those that
     * pass the predicate (i.e. for which the Predicate.filter() returns true.)
     * 
     * @return The next tuple that passes the filter, or null if there are no
     *         more tuples
     * @see Predicate#filter
     */
    protected Tuple fetchNext() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        if (itr != null && itr.hasNext()) {
            return itr.next();
        } else {
            return null;
        }
    }

    @Override
    public DbIterator[] getChildren() {
        return new DbIterator[] {child};
    }

    @Override
    public void setChildren(DbIterator[] children) {
        child = children[0];
    }

}

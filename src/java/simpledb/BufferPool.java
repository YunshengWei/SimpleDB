package simpledb;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Random;
import java.util.Set;


/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 */
public class BufferPool {
    /** Bytes per page, including header. */
    public static final int PAGE_SIZE = 4096;

    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;
    
    /** Maximum number of pages in buffer pool. */
    private int numPages;
    /** Pointers to buffer pool pages. */
    private Page[] bufferedPages;
    /** Records the mapping between PageId and its location in buffer pool. */
    private HashMap<PageId, Integer> pageLookupTable;
    /** Records free pages in buffer pool. */
    private LinkedList<Integer> freeList;
    /** Used for random eviction policy. */
    private Random rnd;
    /** Manages transactions and locks. */
    private LockManager lockManager;
    /** Maintains buffer pool page index which is occupied and clean. */
    private Set<Integer> cleanPages;
    /** how long is it considered as a deadlock */
    private static final int TIMEOUT = 2000;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        this.numPages = numPages;
        bufferedPages = new Page[numPages];
        freeList = new LinkedList<Integer>();
        for (int i = 0; i < numPages; i++) {
            freeList.add(i);
        }
        pageLookupTable = new HashMap<PageId, Integer>();
        rnd = new Random();
        lockManager = new LockManager(BufferPool.TIMEOUT);
        cleanPages = new HashSet<Integer>();
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, an page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
        // Assume that tid == null is only used by test case and database system itself,
        // so we do not acquire lock for null tid.
        if (tid != null) {
            try {
            if (perm == Permissions.READ_ONLY) {
                lockManager.acquireReadLock(tid, pid);
            } else if (perm == Permissions.READ_WRITE) {
                lockManager.acquireWriteLock(tid, pid);
            } else {
                // Should never reach here
                assert false : "should never reach here.";
                System.exit(-1);
            }
            } catch (TransactionAbortedException e) {
                System.err.println(tid);
                throw new TransactionAbortedException();
            } catch (InterruptedException e) {
                e.printStackTrace();
                System.exit(-1);
            }
        }
        synchronized (this) {
            Integer loc = pageLookupTable.get(pid);
            if (loc != null) {
                return bufferedPages[loc];
            }
            if (freeList.isEmpty()) {
                evictPage();
            }
            int newLoc = freeList.pop();
            int tableId = pid.getTableId();
            if (pid.pageNumber() < ((HeapFile) Database.getCatalog().getDbFile(tableId)).numPages()) {
                bufferedPages[newLoc] = Database.getCatalog().getDbFile(tableId).readPage(pid);
            } else {
                // if page is not in the heapfile, first allocate a new page in buffer pool
                // rather than directly add a new page to the heapfile, which is needed to 
                // support NO-STEAL policy
                try {
                    bufferedPages[newLoc] = new HeapPage((HeapPageId) pid, HeapPage.createEmptyPageData());
                } catch (IOException e) {
                    throw new DbException("Some internal errors happen.");
                }
            }
            cleanPages.add(newLoc);
            pageLookupTable.put(pid, newLoc);
            return bufferedPages[newLoc];
        }
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public void releasePage(TransactionId tid, PageId pid) {
        lockManager.releasePage(tid, pid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        transactionComplete(tid, true);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId pid) {
        return lockManager.holdsLock(tid, pid);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit)
        throws IOException {
        if (commit) {
            flushPages(tid);
        } else {
            synchronized (this) {
                for (PageId pid : lockManager.getAllLockingPages(tid)) {
                    // Since we adopt NOSTEAL policy,
                    // pid is not in pageLookupTable can infer that
                    // pid is not dirty
                    if (pageLookupTable.containsKey(pid)) {
                        int i = pageLookupTable.get(pid);
                        HeapFile hf = (HeapFile) Database.getCatalog().getDbFile(pid.getTableId());
                        if (pid.pageNumber() >= hf.getDiskFileNumPages()) {
                            hf.resetNumPages();
                        }
                        
                        if (bufferedPages[i].isDirty() != null) {
                            bufferedPages[i] = bufferedPages[i].getBeforeImage();
                            cleanPages.add(i);
                        }
                    }
                }
            }
        }
        lockManager.releaseAllLocks(tid);
    }

    /**
     * Add a tuple to the specified table behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to(Lock 
     * acquisition is not needed for lab2). May block if the lock cannot 
     * be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and updates cached versions of any pages that have 
     * been dirtied so that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        Database.getCatalog().getDbFile(tableId).insertTuple(tid, t);
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from. May block if
     * the lock cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit.  Does not need to update cached versions of any pages that have 
     * been dirtied, as it is not possible that a new page was created during the deletion
     * (note difference from addTuple).
     *
     * @param tid the transaction adding the tuple.
     * @param t the tuple to add
     */
    public void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, TransactionAbortedException {
        int tableid = t.getRecordId().getPageId().getTableId();
        Database.getCatalog().getDbFile(tableid).deleteTuple(tid, t);
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        for (Page page : bufferedPages) {
            if (page != null) {
                flushPage(page.getId());
            }
        }
    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
    */
    public synchronized void discardPage(PageId pid) {
        try {
            flushPage(pid);
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(-1);
        }
        Integer loc = pageLookupTable.get(pid);
        if (loc != null) {
            bufferedPages[loc] = null;
            pageLookupTable.remove(pid);
            freeList.add(loc);
            cleanPages.remove(loc);
        }
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized void flushPage(PageId pid) throws IOException {
        Integer i = pageLookupTable.get(pid);
        if (i == null) {
            return;
        }
        Page page = bufferedPages[i];
        if (page.isDirty() != null) {
            Database.getCatalog().getDbFile(pid.getTableId()).writePage(page);
            page.markDirty(false, null);
        }
        cleanPages.add(i);
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized void flushPages(TransactionId tid) throws IOException {
        for (PageId pid : lockManager.getAllLockingPages(tid)) {
            flushPage(pid);
        }
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized void evictPage() throws DbException {
        // For simplicity, only implement random eviction policy;
        if (cleanPages.size() == 0) {
            throw new DbException("All pages in the bufferpool are dirty.");
        }
        int i = rnd.nextInt(cleanPages.size());
        int j = 0;
        int evictLoc = 0;
        for (int k : cleanPages) {
            if (j == i) {
                evictLoc = k;
                break;
            }
            j++;
        }
        
        PageId pid = bufferedPages[evictLoc].getId();
        discardPage(pid);
    }
    
    /**
     * Inform the buffer pool that a specified page will be modified to avoid
     * the page being evicted.
     * 
     * @param pid
     *            the id of the specified page.
     *            Require the page to be in the buffer pool.
     */
    synchronized void markDirty(PageId pid) {
        Integer i = pageLookupTable.get(pid);
        cleanPages.remove(i);
    }
}

package simpledb;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
//import java.util.concurrent.locks.Lock;
//import java.util.concurrent.locks.ReadWriteLock;
//import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * LockManager is responsible for maintaining state about transactions and
 * locks.
 */
public class LockManager {
    
    /** Maps PageId to its associated ReadWriteLock */
    private ConcurrentMap<PageId, ReadWriteLock> lockMap;
    /** Maintains information about which locks a transaction holds */
    private ConcurrentMap<TransactionId, Set<ReadWriteLock>> transLocksMap;

    public LockManager() {
        lockMap = new ConcurrentHashMap<PageId, ReadWriteLock>();
        transLocksMap = new ConcurrentHashMap<TransactionId, Set<ReadWriteLock>>();
    }
    
    /**
     * @param pid
     *            the ID of the page to get lock for
     * @return the lock associated with the page
     */
    public ReadWriteLock getReadWriteLock(PageId pid) {
        ReadWriteLock rwl = lockMap.get(pid);
        if (rwl == null) {
            lockMap.putIfAbsent(pid, new ReadWriteLock());
            rwl = lockMap.get(pid);
        }
        return rwl;
    }

    /**
     * Acquire read lock for a page on behalf of a transaction. May block
     * current thread until lock is available.
     * 
     * @param tid
     *            id of the transaction requesting the lock
     * @param pid
     *            id of the page to acquire read lock for
     */
    public void acquireReadLock(TransactionId tid, PageId pid) {
        ReadWriteLock rwl = getReadWriteLock(pid);
        try {
            rwl.lockRead(tid);
        } catch (InterruptedException e) {
            e.printStackTrace();
            // Terminate current thread
            throw new RuntimeException(String.format("Thread %s interrupted", Thread.currentThread()));
        }
        Set<ReadWriteLock> locks = transLocksMap
                .getOrDefault(tid, new HashSet<ReadWriteLock>());
        locks.add(rwl);
        transLocksMap.put(tid, locks);
    }

    /**
     * Acquire write lock for a page on behalf of a transaction. May block
     * current thread until lock is available.
     * 
     * @param tid
     *            id of the transaction requesting the lock
     * @param pid
     *            id of the page to acquire write lock for
     */
    public void acquireWriteLock(TransactionId tid, PageId pid) {
        ReadWriteLock rwl = getReadWriteLock(pid);
        // Must release read lock before acquiring write lock
        Set<ReadWriteLock> locks = transLocksMap
                .getOrDefault(tid, new HashSet<ReadWriteLock>());
        try {
            rwl.lockWrite(tid);
        } catch (InterruptedException e) {
            e.printStackTrace();
            // Terminate current thread
            throw new RuntimeException(String.format("Thread %s interrupted", Thread.currentThread()));
        }
        locks.add(rwl);
        transLocksMap.put(tid, locks);
    }

    /**
     * Release the lock on a page.
     * 
     * @param tid
     *            the ID of the transaction requesting the unlock
     * @param pid
     *            the ID of the page to unlock
     */
    public void releasePage(TransactionId tid, PageId pid) {
        Set<ReadWriteLock> locks = transLocksMap.getOrDefault(tid, new HashSet<ReadWriteLock>());
        ReadWriteLock rwl = getReadWriteLock(pid);
        if (locks.contains(rwl)) {
            locks.remove(rwl);
            if (rwl.isReader(tid)) {
                rwl.unlockRead(tid);
            }
            if (rwl.isWriter(tid)) {
                rwl.unlockWrite(tid);
            }
            transLocksMap.put(tid, locks);
        }
    }
    
    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId pid) {
        ReadWriteLock rwl = getReadWriteLock(pid);
        Set<ReadWriteLock> locks = transLocksMap.getOrDefault(tid, new HashSet<ReadWriteLock>());
        
        if (locks.contains(rwl)) {
            return true;
        } else {
            return false;
        }
    }
}
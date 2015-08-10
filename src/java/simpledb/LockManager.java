package simpledb;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * LockManager is responsible for maintaining state about transactions and
 * locks. Assume one transaction can be executed in several threads.
 */
public class LockManager {

    /** how long is it considered as a deadlock */
    private final int timeout;
    /** Maps PageId to its associated ReadWriteLock */
    private final ConcurrentMap<PageId, ReadWriteLock> lockMap;
    /** Maintains information about which locks a transaction holds and requests */
    private final ConcurrentMap<TransactionId, Set<ReadWriteLock>> holdingAndRuestignLocks;

    public LockManager(int timeout) {
        this.timeout = timeout;
        lockMap = new ConcurrentHashMap<PageId, ReadWriteLock>();
        holdingAndRuestignLocks = new ConcurrentHashMap<TransactionId, Set<ReadWriteLock>>();
    }

    /**
     * @param pid
     *            the ID of the page to get lock for
     * @return the lock associated with the page
     */
    public ReadWriteLock getReadWriteLock(PageId pid) {
        ReadWriteLock rwl = lockMap.get(pid);
        if (rwl == null) {
            lockMap.putIfAbsent(pid, new ReadWriteLock(pid, timeout));
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
     * @throws InterruptedException
     * @throws TransactionAbortedException
     */
    public void acquireReadLock(TransactionId tid, PageId pid)
            throws InterruptedException, TransactionAbortedException {
        synchronized (tid) {
            ReadWriteLock rwl = getReadWriteLock(pid);
            Set<ReadWriteLock> locks = holdingAndRuestignLocks.getOrDefault(tid,
                    new HashSet<ReadWriteLock>());
            locks.add(rwl);
            rwl.lockRead(tid);
            holdingAndRuestignLocks.put(tid, locks);
        }
    }

    /**
     * Acquire write lock for a page on behalf of a transaction. May block
     * current thread until lock is available.
     * 
     * @param tid
     *            id of the transaction requesting the lock
     * @param pid
     *            id of the page to acquire write lock for
     * @throws InterruptedException
     * @throws TransactionAbortedException
     */
    public void acquireWriteLock(TransactionId tid, PageId pid)
            throws InterruptedException, TransactionAbortedException {
        synchronized (tid) {
            ReadWriteLock rwl = getReadWriteLock(pid);
            Set<ReadWriteLock> locks = holdingAndRuestignLocks.getOrDefault(tid,
                    new HashSet<ReadWriteLock>());
            locks.add(rwl);
            rwl.lockWrite(tid);
            holdingAndRuestignLocks.put(tid, locks);
        }
    }

    /**
     * Release the holding and requesting lock on a page.
     * 
     * @param tid
     *            the ID of the transaction requesting the unlock
     * @param pid
     *            the ID of the page to unlock
     */
    public void releaseLockAndRequest(TransactionId tid, PageId pid) {
        synchronized (tid) {
            Set<ReadWriteLock> locks = holdingAndRuestignLocks.getOrDefault(tid,
                    Collections.emptySet());
            ReadWriteLock rwl = getReadWriteLock(pid);
            if (locks.contains(rwl)) {
                locks.remove(rwl);
                if (rwl.isReader(tid)) {
                    rwl.unlockRead(tid);
                } 
                if (rwl.isWriter(tid)) {
                    rwl.unlockWrite(tid);
                }
                rwl.cancelLockRequests(tid);
                holdingAndRuestignLocks.put(tid, locks);
            }
        }
    }

    /**
     * Return true if the specified transaction holds a lock on the specified
     * page
     */
    public boolean holdsLock(TransactionId tid, PageId pid) {
        synchronized (tid) {
            ReadWriteLock rwl = getReadWriteLock(pid);
            Set<ReadWriteLock> locks = holdingAndRuestignLocks.getOrDefault(tid,
                    Collections.emptySet());

            if (locks.contains(rwl) && (rwl.isReader(tid) || rwl.isWriter(tid))) {
                return true;
            } else {
                return false;
            }
        }
    }

    /**
     * Release all the locks the specified transaction holds and cancel
     * all the locking requests the specified transaction issues.
     * 
     * @param tid
     *            the Id of the transaction
     */
    public void releaseAllLocksAndRequests(TransactionId tid) {
        synchronized (tid) {
            Set<ReadWriteLock> locks = holdingAndRuestignLocks.getOrDefault(tid,
                    Collections.emptySet());
            for (ReadWriteLock rwl : locks) {
                if (rwl.isReader(tid)) {
                    rwl.unlockRead(tid);
                } 
                if (rwl.isWriter(tid)) {
                    rwl.unlockWrite(tid);
                }
                rwl.cancelLockRequests(tid);
            }
            holdingAndRuestignLocks.put(tid, new HashSet<ReadWriteLock>());
        }
    }

    /**
     * @param tid
     *            the id of the transaction
     * @return an iterable of all PageId the specified transaction holds locks
     *         on
     */
    public Iterable<PageId> getAllLockingPages(TransactionId tid) {
        synchronized (tid) {
            Set<ReadWriteLock> locks = holdingAndRuestignLocks.getOrDefault(tid,
                    Collections.emptySet());
            List<PageId> plist = new ArrayList<PageId>();
            for (ReadWriteLock lock : locks)
            {
                if (lock.isReader(tid) || lock.isWriter(tid)) {
                    plist.add(lock.getPageId());
                }
            }
            return plist;
        }
    }
}

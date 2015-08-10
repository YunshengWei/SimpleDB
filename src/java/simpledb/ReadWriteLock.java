package simpledb;

import java.util.HashSet;
import java.util.Set;

/**
 * ReadWriteLock is a modified Reentrant Lock implementation which supports
 * transaction. It can support several threads running one transaction.
 * ReadWriteLock proritizes write request over read request. ReadWriteLock adopt
 * a timeout based method to detect deadlocks (A better method is waits-for
 * graph, but much more complicated).
 */
public class ReadWriteLock {

    /** Indicate which PageId this ReadWriteLock is associated with */
    private PageId pid;

    private Set<TransactionId> readingTids = new HashSet<>();
    private Set<TransactionId> writeRequestSet = new HashSet<>();
    private TransactionId writingTid = null;
    // how long is it considered a deadlock
    private final int timeout;

    public ReadWriteLock(PageId pid, int timeout) {
        this.pid = pid;
        this.timeout = timeout;
    }

    /**
     * @return the PageId this lock is associated with
     */
    public PageId getPageId() {
        return pid;
    }

    public synchronized void cancelLockRequests(TransactionId tid) {
        writeRequestSet.remove(tid);
    }
    
    public synchronized void lockRead(TransactionId tid)
            throws InterruptedException, TransactionAbortedException {
        while (!canGrantReadAccess(tid)) {
            long startTime = System.currentTimeMillis();
            wait(timeout);
            if (System.currentTimeMillis() - startTime >= timeout) {
                throw new TransactionAbortedException();
            }
        }

        readingTids.add(tid);
    }

    private boolean canGrantReadAccess(TransactionId tid) {
        if (isWriter(tid))
            return true;
        if (hasWriter())
            return false;
        if (isReader(tid))
            return true;
        if (hasWriteRequests())
            return false;
        return true;
    }

    public synchronized void unlockRead(TransactionId tid) {
        if (!isReader(tid)) {
            throw new IllegalMonitorStateException(
                    "Calling TransactionId does not"
                            + " hold a read lock on this ReadWriteLock");
        }
        readingTids.remove(tid);
        notifyAll();
    }

    public synchronized void lockWrite(TransactionId tid)
            throws InterruptedException, TransactionAbortedException {
        writeRequestSet.add(tid);
        while (!canGrantWriteAccess(tid)) {
            long startTime = System.currentTimeMillis();
            wait(timeout);
            if (System.currentTimeMillis() - startTime >= timeout) {
                throw new TransactionAbortedException();
            }
        }
        writeRequestSet.remove(tid);
        writingTid = tid;
    }

    public synchronized void unlockWrite(TransactionId tid) {
        if (!isWriter(tid)) {
            throw new IllegalMonitorStateException(
                    "Calling TransactionId does not"
                            + " hold the write lock on this ReadWriteLock");
        }
        writingTid = null;
        notifyAll();
    }

    private boolean canGrantWriteAccess(TransactionId tid) {
        if (isOnlyReader(tid))
            return true;
        if (hasReaders())
            return false;
        if (writingTid == null)
            return true;
        if (!isWriter(tid))
            return false;
        return true;
    }

    private boolean hasReaders() {
        return readingTids.size() > 0;
    }

    public synchronized boolean isReader(TransactionId tid) {
        return readingTids.contains(tid);
    }

    private boolean isOnlyReader(TransactionId tid) {
        return readingTids.size() == 1 && readingTids.contains(tid);
    }

    private boolean hasWriter() {
        return writingTid != null;
    }

    public synchronized boolean isWriter(TransactionId tid) {
        return writingTid == tid;
    }

    private boolean hasWriteRequests() {
        return writeRequestSet.size() > 0;
    }

}
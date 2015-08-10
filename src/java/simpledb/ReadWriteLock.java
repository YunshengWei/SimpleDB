package simpledb;

import java.util.HashMap;
import java.util.Map;

/**
 * ReadWriteLock is a modified Reentrant Lock implementation which supports
 * transaction. It can support several threads running one transaction.
 */
public class ReadWriteLock {

    /** Indicate which PageId this ReadWriteLock is associated with */
    private PageId pid;

    private Map<TransactionId, Integer> readingTids = new HashMap<TransactionId, Integer>();
    private int writeAccesses = 0;
    private int writeRequests = 0;
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

    public synchronized void lockRead(TransactionId tid)
            throws InterruptedException, TransactionAbortedException {
        while (!canGrantReadAccess(tid)) {
            long startTime = System.currentTimeMillis();
            wait(timeout);
            if (System.currentTimeMillis() - startTime >= timeout) {
                throw new TransactionAbortedException();
            }
        }

        readingTids.put(tid, 1);
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
        writeRequests++;
        while (!canGrantWriteAccess(tid)) {
            long startTime = System.currentTimeMillis();
            wait(timeout);
            if (System.currentTimeMillis() - startTime >= timeout) {
                throw new TransactionAbortedException();
            }
        }
        writeRequests--;
        writeAccesses = 1;
        writingTid = tid;
    }

    public synchronized void unlockWrite(TransactionId tid) {
        if (!isWriter(tid)) {
            throw new IllegalMonitorStateException(
                    "Calling TransactionId does not"
                            + " hold the write lock on this ReadWriteLock");
        }
        writeAccesses = 0;
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
        return readingTids.get(tid) != null;
    }

    private boolean isOnlyReader(TransactionId tid) {
        return readingTids.size() == 1 && readingTids.get(tid) != null;
    }

    private boolean hasWriter() {
        return writingTid != null;
    }

    public synchronized boolean isWriter(TransactionId tid) {
        return writingTid == tid;
    }

    private boolean hasWriteRequests() {
        return this.writeRequests > 0;
    }

}
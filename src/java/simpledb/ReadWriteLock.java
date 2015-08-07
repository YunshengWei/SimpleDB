package simpledb;

import java.util.HashMap;
import java.util.Map;

/**
 * ReadWriteLock is a modified Reentrant Lock implementation which supports
 * transaction. It can support several threads running one transaction.
 */
public class ReadWriteLock {

    private Map<TransactionId, Integer> readingTids = new HashMap<TransactionId, Integer>();

    private int writeAccesses = 0;
    private int writeRequests = 0;
    private TransactionId writingTid = null;

    public synchronized void lockRead(TransactionId tid)
            throws InterruptedException {
        while (!canGrantReadAccess(tid)) {
            wait();
        }

        readingTids.put(tid, (getReadAccessCount(tid) + 1));
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
        int accessCount = getReadAccessCount(tid);
        if (accessCount == 1) {
            readingTids.remove(tid);
        } else {
            readingTids.put(tid, accessCount - 1);
        }
        notifyAll();
    }

    public synchronized void lockWrite(TransactionId tid)
            throws InterruptedException {
        writeRequests++;
        while (!canGrantWriteAccess(tid)) {
            wait();
        }
        writeRequests--;
        writeAccesses++;
        writingTid = tid;
    }

    public synchronized void unlockWrite(TransactionId tid) {
        if (!isWriter(tid)) {
            throw new IllegalMonitorStateException(
                    "Calling TransactionId does not"
                            + " hold the write lock on this ReadWriteLock");
        }
        writeAccesses--;
        if (writeAccesses == 0) {
            writingTid = null;
        }
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

    private int getReadAccessCount(TransactionId tid) {
        Integer accessCount = readingTids.get(tid);
        if (accessCount == null)
            return 0;
        return accessCount.intValue();
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
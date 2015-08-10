package simpledb;

import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Set;

public class ReadWriteLock {

    // nested enum is implicitly static
    private static enum LockType {
        READ, WRITE;
    }
    
    private static class LockRequest {
        TransactionId tid;
        LockType lt;
        
        LockRequest(TransactionId tid, LockType lt) {
            this.tid = tid;
            this.lt = lt;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == this) {
                return true;
            }
            if (!(obj instanceof LockRequest)) {
                return false;
            } else {
                LockRequest that = (LockRequest) obj;
                return that.tid.equals(this.tid) && that.lt == this.lt;
            }
        }

        @Override
        public int hashCode() {
            // TODO Auto-generated method stub
            return super.hashCode();
        }
    }
    
    /** Indicate which PageId this ReadWriteLock is associated with */
    private PageId pid;

    private Set<TransactionId> currentlyAccessTids = new HashSet<>();
    private LockType currentlyHeldLockType;
    private Queue<LockRequest> lockRequestQueue = new LinkedList<>();
    
    private Set<TransactionId> readingTids = new HashSet<>();
    private int writeAccesses = 0;
    private int writeRequests = 0;
    private TransactionId writingTid = null;
    private final WaitsForGraph wfGraph;
    private final Set<TransactionId> writeRequestSet = new HashSet<>();

    public ReadWriteLock(PageId pid, WaitsForGraph wfGraph) {
        this.pid = pid;
        this.wfGraph = wfGraph;
    }

    /**
     * @return the PageId this lock is associated with
     */
    public PageId getPageId() {
        return pid;
    }

    public synchronized void lockRead(TransactionId tid)
            throws InterruptedException, TransactionAbortedException {
        if (isWriter(tid) || isReader(tid)) {
            return;
        }
        
        // Very Very Tricky here!!!
        LockRequest lockRequest = new LockRequest(tid, LockType.READ);
        lockRequestQueue.add(lockRequest);
        while (true) {
            boolean canGrantReadAccess = true;
            if (currentlyHeldLockType == LockType.WRITE) {
                canGrantReadAccess = false;
            } else {
                for (LockRequest lr : lockRequestQueue) {
                    if (lr.lt == LockType.WRITE) {
                        canGrantReadAccess = false;
                        break;
                    }
                }
            }
            if (canGrantReadAccess) {
                lockRequestQueue.remove(lockRequest);
                currentlyHeldLockType = LockType.READ;
                currentlyAccessTids.add(tid);
            } else {
                wait();
            }
        }
        
    }
    
    public synchronized void lockWrite(Transaction tid) {
        
    }

    private Set<TransactionId> canGrantReadAccess(TransactionId tid) {
        if (isWriter(tid))
            return Collections.emptySet();
        if (hasWriter())
            return Collections.singleton(writingTid);
        if (isReader(tid))
            return Collections.emptySet();
        if (hasWriteRequests())
            return writeRequestSet;
        return Collections.emptySet();;
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
        while (canGrantWriteAccess(tid) != null) {
            
            wait();
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

    /**
     * @return the set of transactions a specified transaction is waiting for
     */
    private Set<TransactionId> canGrantWriteAccess(TransactionId tid) {
        if (isOnlyReader(tid))
            return Collections.emptySet();
        if (hasReaders())
            return readingTids;
        if (writingTid == null)
            return Collections.emptySet();
        if (!isWriter(tid))
            return Collections.singleton(writingTid);
        return Collections.emptySet();;
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
        return this.writeRequests > 0;
    }

}
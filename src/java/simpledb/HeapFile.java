package simpledb;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    private static final long serialVersionUID = 1L;
    
    private TupleDesc td;
    private File file;
    // In order to support NO-STEAL policy, we need a variable to record
    // current number of pages of the HeapFile.
    // When a new page is added to the HeapFile, the actual number of pages
    // on the disk and the true number of pages are different.
    private int numPages;
    
    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        this.file = f;
        this.td = td;
        this.numPages = (int) (file.length() / BufferPool.PAGE_SIZE);
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        return file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        return file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        try (RandomAccessFile raf = new RandomAccessFile(file, "r")) {
            long pos = pid.pageNumber() * (long) BufferPool.PAGE_SIZE;
            if (pos < 0 || pos >= file.length()) {
                throw new IllegalArgumentException("The page doesn't exist in this file.");
            }
            
            raf.seek(pos);
            byte[] buf = new byte[BufferPool.PAGE_SIZE];
            raf.read(buf);
            return new HeapPage((HeapPageId) pid, buf);
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        }
        // should never reach here.
        return null;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        RandomAccessFile raf = new RandomAccessFile(file, "rw");
        
        PageId pid = page.getId();
        long pos = pid.pageNumber() * (long) BufferPool.PAGE_SIZE;
        // if pos is beyond the end of file => new pages need to be appended to the file
        raf.seek(pos);
        raf.write(page.getPageData());
        raf.close();
    }

    /**
     * All access to numPages variable should use this synchronized method.
     * @return the number of pages in this HeapFile.
     */
    public synchronized int numPages() {
        return numPages;
    }
    
    /**
     * Increment numPages by 1.
     * All modification to numPages variable should use this synchronized method.
     */
    private synchronized void incrementNumPages() {
        numPages++;
    }
    
    /**
     * Reset numPages to the number of pages of the heapfile on disk.
     * All modification to numPages variable should use this synchronized method.
     * This method is used by BufferPool.transactionComplete() to ensure aborted
     * transactions which add new pages do not have actual effects on the heapfile.
     */
    public synchronized void resetNumPages() {
        numPages = (int) (file.length() / BufferPool.PAGE_SIZE);
    }

    /**
     * @return the actual number of pages of the heapfile on disk 
     */
    public int getDiskFileNumPages() {
        return (int) (file.length() / BufferPool.PAGE_SIZE);
    }
    
    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        
        ArrayList<Page> pages = new ArrayList<Page>();
        
        for (int i = 0; ; i++) {
            HeapPageId pid = new HeapPageId(getId(), i);
            // acquire READ_ONLY lock first
            HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid,
                    pid, Permissions.READ_ONLY);
            if (page.getNumEmptySlots() == 0) {
                Database.getBufferPool().releasePage(tid, pid);
                continue;
            }
            
            synchronized (Database.getBufferPool()) {
                page = (HeapPage) Database.getBufferPool().getPage(tid,
                        pid, Permissions.READ_WRITE);
                // Must first inform buffer pool that the page will be modified to avoid being evicted
                Database.getBufferPool().markDirty(pid);
            }
            
            page.markDirty(true, tid);
            page.insertTuple(t);
            pages.add(page);
            
            if (i == numPages()) {
                incrementNumPages();
            }
            
            return pages;
        }
    }

    // see DbFile.java for javadocs
    public Page deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        
        PageId pid = t.getRecordId().getPageId();
        if (pid.getTableId() != getId() || pid.pageNumber() >= numPages()) {
            throw new DbException("The tuple is not a member of the file");
        }
        
        HeapPage page;
        // ensure that between getPage() and markDirty(), 
        // the page will not be evicted
        synchronized (Database.getBufferPool()) {
            page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
            // Must first inform buffer pool that the page will be modified to avoid being evicted
            Database.getBufferPool().markDirty(pid);Database.getBufferPool().markDirty(pid);
        }
        page.markDirty(true, tid);
        page.deleteTuple(t);
        return page;
    }

    private class HeapFileIterator implements DbFileIterator {
        
        private static final long serialVersionUID = 1L;
        
        private int curPage = 0;
        private Iterator<Tuple> curItr = null;
        private TransactionId tid;
        private boolean open = false;;
        
        public HeapFileIterator(TransactionId tid) {
            this.tid = tid;
        }
        
        @Override
        public void open() throws DbException, TransactionAbortedException {
            open = true;
            curPage = 0;
            if (curPage >= numPages()) {
                return;
            }
            curItr = ((HeapPage) Database.getBufferPool().getPage(tid,
                    new HeapPageId(getId(), curPage), Permissions.READ_ONLY))
                    .iterator();
            advance();
        }

        private void advance() throws TransactionAbortedException, DbException {
            while (!curItr.hasNext()) {
                curPage++;
                if (curPage < numPages()) {
                    curItr = ((HeapPage) Database.getBufferPool().getPage(tid,
                            new HeapPageId(getId(), curPage),
                            Permissions.READ_ONLY)).iterator();
                } else {
                    break;
                }
            }
        }

        @Override
        public boolean hasNext() throws DbException,
                TransactionAbortedException {
            if (!open) {
                return false;
            }
            return curPage < numPages();
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException,
                NoSuchElementException {
            if (!open) {
                throw new NoSuchElementException("iterator not open.");
            }
            if (!hasNext()) {
                throw new NoSuchElementException("No more tuples.");
            }
            Tuple result = curItr.next();
            advance();
            return result;
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            if (!open) {
                throw new DbException("iterator not open yet.");
            }
            close();
            open();          
        }

        @Override
        public void close() {
            curItr = null;
            curPage = 0;
            open = false;
        }

    }
    
    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        return new HeapFileIterator(tid);
    }

}


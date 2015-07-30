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

    private TupleDesc td;
    private File file;
    
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
        // some code goes here
        // not necessary for proj1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        return (int) (file.length() / BufferPool.PAGE_SIZE);
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        
        ArrayList<Page> pages = new ArrayList<Page>();
        HeapPageId pid = null;
        HeapPage page = null;
        
        for (int i = 0; i < numPages(); i++) {
            pid = new HeapPageId(getId(), i);
            // initially acquire a READ lock
            page = (HeapPage) Database.getBufferPool().getPage(tid,
                    pid, Permissions.READ_ONLY);
            if (page.getNumEmptySlots() > 0) {
                break;
            }
            Database.getBufferPool().releasePage(tid, pid);
            pid = null;
            page = null;
        }
        
        if (pid == null) {
            FileOutputStream fos = new FileOutputStream(file, true);
            fos.write(HeapPage.createEmptyPageData());
            fos.close();
            pid = new HeapPageId(getId(), numPages() - 1);
        }
        
        page = (HeapPage) Database.getBufferPool().getPage(tid,
                pid, Permissions.READ_WRITE);
        page.insertTuple(t);
        pages.add(page);
        page.markDirty(true, tid);
        Database.getBufferPool().releasePage(tid, pid);
        return pages;
    }

    // see DbFile.java for javadocs
    public Page deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        PageId pid = t.getRecordId().getPageId();
        if (pid.getTableId() != getId() || pid.pageNumber() >= numPages()) {
            throw new DbException("The tuple is not a member of the file");
        }
        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
        page.deleteTuple(t);
        page.markDirty(true, tid);
        Database.getBufferPool().releasePage(tid, pid);
        return page;
    }

    private class HeapFileIterator implements DbFileIterator {
        
        private static final long serialVersionUID = 1L;
        
        private int curPage;
        private Iterator<Tuple> curItr;
        private TransactionId tid;
        private BufferPool bp;
        private boolean open;
        
        public HeapFileIterator(TransactionId tid) {
            this.tid = tid;
            open = false;
            bp = Database.getBufferPool();
        }
        
        @Override
        public void open() throws DbException, TransactionAbortedException {
            open = true;
            curPage = 0;
            if (curPage >= numPages()) {
                return;
            }
            curItr = ((HeapPage) bp.getPage(tid, new HeapPageId(getId(), curPage), Permissions.READ_ONLY)).iterator();
            advance();
        }
        
        private void advance() throws TransactionAbortedException, DbException {
            while (!curItr.hasNext()) {
                curPage++;
                if (curPage < numPages()) {
                    curItr = ((HeapPage) bp.getPage(tid, new HeapPageId(getId(), curPage), Permissions.READ_ONLY)).iterator();
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
                throw new DbException("iterator not open.");
            }
            open();          
        }

        @Override
        public void close() {
            open = false;
        }
        
    }
    
    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        return new HeapFileIterator(tid);
    }

}


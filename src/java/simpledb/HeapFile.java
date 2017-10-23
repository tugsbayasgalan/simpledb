package simpledb;

import java.io.*;
import java.util.*;


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


	private final File file;
	private final TupleDesc td;

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

    	return this.file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     *
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        return this.file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     *
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here

    	// this is gonna be long

    	// useful variables
    	int tableId = pid.getTableId();
    	int numPage = pid.getPageNumber();
    	int pageSize = BufferPool.getPageSize();
    	int totalSize = pageSize * numPage;
    	
    	

    	try {
			RandomAccessFile raf = new RandomAccessFile(this.file, "r");

			byte[] readData = new byte[pageSize];
			
			raf.seek(totalSize);
			
			int readBytes = raf.read(readData, 0, pageSize);
			

			if (readBytes == pageSize) {

				HeapPageId pageId= new HeapPageId(tableId, numPage);
				HeapPage page = new HeapPage(pageId, readData);
				raf.close();
				return page;

			}

			else {
				raf.close();
				throw new RuntimeException("It didn't read everything");
				
			}
			
			


		} catch (IOException e) {
			// TODO Auto-generated catch block
			throw new RuntimeException("Something is wrong with formatting");
		} 

    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
    	
    	int offset = page.getId().getPageNumber() * BufferPool.getPageSize();
    	
    	RandomAccessFile raf = new RandomAccessFile(this.file, "rw");
    	raf.seek((long) offset);
    	raf.write(page.getPageData());
    	
    	raf.close();
    	
    	

    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        int pageSize = BufferPool.getPageSize();

        int numPages = (int) Math.ceil(this.getFile().length() * 1.0/ pageSize);
        

        return numPages;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
    	
    	ArrayList<Page> result = new ArrayList<>();
    	
    	int numPages = numPages();
    	
    	
    	HeapPageId pid = new HeapPageId(this.getId(), numPages - 1);
    	
    	HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
    	
    	if (page.getNumEmptySlots() == 0){
    		
    		
    		
    		HeapPageId newPid = new HeapPageId(this.getId(), numPages);
    		
    		byte[] data = new byte[BufferPool.getPageSize()];
    		
    		HeapPage newPage = new HeapPage(newPid, data);
    		writePage(newPage);
    		page = (HeapPage) Database.getBufferPool().getPage(tid, newPid, Permissions.READ_WRITE);
    		
    	}
    	
    
    		
    	page.insertTuple(t);
        result.add(page);
        return result;
    		
    	
    	
    	
    	

    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
    	
    	ArrayList<Page> result = new ArrayList<>();
    	
    	HeapPageId tuplePageId = (HeapPageId) t.getRecordId().getPageId();
    	HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, tuplePageId, Permissions.READ_WRITE);
    	page.deleteTuple(t);
    	
    	result.add(page);
    	return result;
    	

    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {

        return new HeapFileIterator(tid, this.numPages(), this.getId());
    }



}

package simpledb;

import java.util.Iterator;
import java.util.NoSuchElementException;

public class HeapFileIterator implements DbFileIterator {

	private final TransactionId transId;
	private final int tableId;
	private boolean openStatus;
	private final int numPages;
	private int curPageNo;
	private Iterator<Tuple> curIterator;


	public HeapFileIterator(TransactionId transId, int numPages, int tableId) {
		this.tableId = tableId;
		this.transId = transId;
		this.numPages = numPages;
		this.openStatus = false;
		this.curPageNo = 0;


	}
	@Override
	public void open() throws DbException, TransactionAbortedException {

		if (this.openStatus) {
			throw new DbException("Iterator is open before");
		}

		this.openStatus = true;
		this.curIterator = getIterator(curPageNo);



	}

	public Iterator<Tuple> getIterator(int pageNo) throws DbException, TransactionAbortedException {
		try {
			
			HeapPage page = (HeapPage) getPage(pageNo);
			return page.iterator();
		} catch (Exception e) {
			System.out.println(e);
			throw new DbException("Something went wrong. Maybe page number is not correct");
		}

	}

	public Page getPage(int pageNo) throws DbException, TransactionAbortedException {

		if (pageNo >= numPages || pageNo < 0){
		
			throw new DbException("Wrong page number");

		}
		else {
			HeapPageId pId = new HeapPageId(tableId, pageNo);
			
			
			Page page =  Database.getBufferPool().getPage(transId, pId, Permissions.READ_ONLY);
			
			return page;

		}


	}

	@Override
	public boolean hasNext() throws DbException, TransactionAbortedException {

		if (this.curIterator == null){
			return false;
		}

		if (!this.curIterator.hasNext()){
			
			

			if (this.curPageNo < numPages - 1){
				
				curPageNo++;
				curIterator = getIterator(curPageNo);
				
				boolean t =  curIterator.hasNext();
				
				return t;
			}

			else {
				
				return false;
			}
		}
        
		return curIterator.hasNext();





	}

	@Override
	public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {

		if (!this.hasNext()) {
			throw new NoSuchElementException("The tuple doesn't exist");
		}

		else {
			return curIterator.next();
		}

	}

	@Override
	public void rewind() throws DbException, TransactionAbortedException {
		
		if (this.openStatus) {
			
			this.curPageNo = 0;
			this.curIterator = getIterator(curPageNo);
		
			
		}
			

	}

	@Override
	public void close() {
		this.openStatus = false;
		this.curIterator = null;

	}

}

package simpledb;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;
    private final TransactionId transactionId;
    private OpIterator child;
    private boolean deleted;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, OpIterator child) {
        // some code goes here
    	this.transactionId = t;
    	this.child = child;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        Type[] type = new Type[]{Type.INT_TYPE};
        TupleDesc td = new TupleDesc(type);
        return td;
    }

    public void open() throws DbException, TransactionAbortedException {
        super.open();
        child.open();
    }

    public void close() {
        super.close();
        child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
    	close();
    	open();
    }

    /**
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
    	if (deleted){
    		return null;
    	}
    	
    	this.deleted = true;
    	int count = 0;
        while(child.hasNext()){
        	Tuple tuple = child.next();
        	try {
				Database.getBufferPool().deleteTuple(transactionId, tuple);
				count++;
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
        }
        
        Tuple returnTuple = new Tuple(this.getTupleDesc());
        IntField intField = new IntField(count);
        returnTuple.setField(0, intField);
        return returnTuple;
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        OpIterator[] result = new OpIterator[1];
        result[0] = child;
        return result;
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
    	this.child = children[0];
    }

}

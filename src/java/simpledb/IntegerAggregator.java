package simpledb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    
    private final int gbfield;
    private final Type gbfieldtype;
    private final int afield;
    private final Op what;
    private final Map<Field, Integer> fieldMap;
    private final Map<Field, Integer> counterMap;
    
    private final static StringField NO_GROUP_KEY = new StringField("no group", 2);

    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        this.fieldMap = new HashMap<>();
        this.counterMap = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
    	
    	if(gbfield == NO_GROUPING){
    		
    		IntField tupField = (IntField) tup.getField(afield);
    		int tupValue = tupField.getValue();
    		
    		if (fieldMap.containsKey(NO_GROUP_KEY)){
    			
    			
    			
    			int value = calculate(what, fieldMap.get(NO_GROUP_KEY), tupValue);
    			fieldMap.put(NO_GROUP_KEY, value);
    			
    			counterMap.put(NO_GROUP_KEY, counterMap.get(NO_GROUP_KEY) + 1);
    			
    		} else {
    			fieldMap.put(NO_GROUP_KEY, tupValue);
    			counterMap.put(NO_GROUP_KEY, 1);
    		}
    		
    		
    	}
    	
    	else {
    		
    		
    		if (Type.INT_TYPE.equals(gbfieldtype)){
    			IntField gbField = (IntField) tup.getField(gbfield);
        		IntField tupField = (IntField)tup.getField(afield);
        		if (fieldMap.containsKey(gbField)){
        			
        			
        			int tupValue = tupField.getValue();
        			int value = calculate(what, fieldMap.get(gbField), tupValue);
        			
        			fieldMap.put(gbField, value);
        			counterMap.put(gbField, counterMap.get(gbField) + 1);
        			
        		} else {
        			fieldMap.put(gbField, tupField.getValue()); 
        			
        			counterMap.put(gbField, 1);
        			
        		}
        		
    			
    			
    		} else {
    			
    			StringField gbField = (StringField) tup.getField(gbfield);
        		IntField tupField = (IntField)tup.getField(afield);
        		if (fieldMap.containsKey(gbField)){
        			
        			
        			int tupValue = tupField.getValue();
        			int value = calculate(what, fieldMap.get(gbField), tupValue);
        			
        			fieldMap.put(gbField, value);
        			counterMap.put(gbField, counterMap.get(gbField) + 1);
        			
        		} else {
        			fieldMap.put(gbField, tupField.getValue()); 
        			
        			counterMap.put(gbField, 1);
        			
        		}
    			
    		}
    		
    		
    		
    		
    		
    		
    		
    	}
    	
    	
        
    }

    private int calculate(Op what, Integer integer, int tupValue) {
    	
		switch (what) {
		
		case AVG:
			return integer + tupValue; // it doesn't matter what u return here
		case MIN:
			return Math.min(integer, tupValue);
		case MAX:
			return Math.max(integer, tupValue);
		case SUM:
			
			return integer + tupValue;
		case COUNT:
			return integer + 1;
		default:
			throw new RuntimeException("should never reach here");

		}
		
	}

	/**
     * Create a OpIterator over group aggregate results.
     * 
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        List<Tuple> tuples = new ArrayList<>();
        
 
        
        if (gbfield == NO_GROUPING) {
        	
        	TupleDesc td = new TupleDesc(new Type[]{Type.INT_TYPE});
        	Tuple tuple = new Tuple(td);
        	
        	if (!fieldMap.isEmpty()){
        		
        		int value = fieldMap.get(NO_GROUP_KEY);
        		int denom = counterMap.get(NO_GROUP_KEY);
        		
        		if (Op.AVG.equals(what)){
        			value = value / denom;
        		}
        		
        		if(Op.COUNT.equals(what)){
        			value = denom;
        		}
        		
        		tuple.setField(0, new IntField(value));
        		
        		tuples.add(tuple);
        		
        	}
        	
        	return new TupleIterator(td, tuples);
        	
        	
        }
        
        else {
        	
        	
        	Type[] types = new Type[2];
        	types[0] = gbfieldtype;
        	types[1] = Type.INT_TYPE;
        	
        	TupleDesc td = new TupleDesc(types);
        	
        	for (Field field: fieldMap.keySet()){
        		Tuple tuple = new Tuple(td);
        		
        		int aggregate = fieldMap.get(field);
        		int denom = counterMap.get(field);
        		
        		if (Op.AVG.equals(what)){
        			aggregate = aggregate / denom;
        		}
        		
        		if(Op.COUNT.equals(what)){
        			aggregate = denom;
        		}
        		
        		tuple.setField(0, field);
        		tuple.setField(1, new IntField(aggregate));
        		tuples.add(tuple);
        		
        	}
        	
        	
        	
        	
        	return new TupleIterator(td, tuples);
        	
        	
        }
    }

}

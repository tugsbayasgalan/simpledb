package simpledb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    
    private final int gbfield;
    private final Type gbfieldType;
    private final int afield;
    private final Op what;
    private final Map<Field, Integer>countMap = new HashMap<>();
    
    private static final StringField NO_GROUP_KEY = new StringField("no group key", 6);

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        
    	
    	this.gbfield = gbfield;
    	this.gbfieldType = gbfieldtype;
    	this.afield = afield;
    	if (!what.equals(Op.COUNT)){
    		throw new IllegalArgumentException("Only supports count for string");
    	}
    	
    	this.what = what;
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
    	
    	if (gbfield == NO_GROUPING){
    		
    		if (countMap.containsKey(NO_GROUP_KEY)){
    			countMap.put(NO_GROUP_KEY, countMap.get(NO_GROUP_KEY) + 1);
    		}
    		
    		else {
    			countMap.put(NO_GROUP_KEY, 1);
    		}
    		
    	}
    	
    	else {
    		
    		
			IntField field = (IntField) tup.getField(gbfield);

			if (countMap.containsKey(field)) {
				countMap.put(field, countMap.get(field) + 1);
			}

			else {
				countMap.put(field, 1);
			}
    		
    	}
        
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {
    	
    	List<Tuple> tuples = new ArrayList<>();
        
    	if (gbfield == NO_GROUPING) {
    		
    		TupleDesc td = new TupleDesc(new Type[]{Type.INT_TYPE});
    		
    		Tuple tuple = new Tuple(td);
    		
    		int value = countMap.get(NO_GROUP_KEY);
    		
    		tuple.setField(0, new IntField(value));
    		
    		tuples.add(tuple);
    		
    		return new TupleIterator(td, tuples);
    		
    	} else {
    		
    		Type[] types = new Type[2];
    		
    		types[0] = gbfieldType;
    		
    		types[1] = Type.INT_TYPE;
    		
    		TupleDesc td = new TupleDesc(types);
    		
    		for (Field field: countMap.keySet()){
    			
    			Tuple tuple = new Tuple(td);
    			
    			int value = countMap.get(field);
    			
    			tuple.setField(0, field);
    			
    			tuple.setField(1, new IntField(value));
    			
    			tuples.add(tuple);
    			
    			
    		}
    		
    		return new TupleIterator(td, tuples);
    	}
    	
    	
    }

}

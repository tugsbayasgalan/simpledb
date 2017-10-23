package simpledb;

import java.util.HashSet;
import java.util.Set;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

	
	private int numTuples = 0;
	private final int min;
	private final int max;
	private final int[] buckets;
	private final double width;
    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
	
	
    public IntHistogram(int buckets, int min, int max) {
    	
    	
    	this.min = min;
    	this.max = max + 1;
    	this.buckets = new int[Math.min(max - min + 1, buckets)];
    	
    	this.width = (max - min + 1.0)/this.buckets.length;
    	

    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	
    	if (v >= min && v < max){
    		int index = getIndex(v);
    		//System.out.println(index);
    		buckets[index]++;
    		numTuples++;
    	}
    	
    }

    private int getIndex(int v) {
    	
    	if (v < min || v >= max){
    		throw new IllegalArgumentException("This integer is out of range");
    	}
    	
    	return (int) ((v - min)/width);
		
	}

	/**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
  

    	switch (op){
    		case LESS_THAN:
    			if (v <= min){
    				return 0.0;
    			}
    			
    			else if (v >= max){
    				return 1.0;
    			}
    			
    			else {
    				int index = getIndex(v);
    				
    				double elementCount = 0;
    				
    				for (int i = 0; i < index; i++){
    					elementCount += buckets[i];
    				}
    				
    				double offset = v - min - index*width;
    				
    				elementCount += (offset/width)*buckets[index];
    				
    				return elementCount/numTuples;
    				
    				
    			}
    			
    		case LESS_THAN_OR_EQ:
    			if (v <= min){
    				return 0.0;
    			}
    			
    			else if (v >= max){
    				return 1.0;
    			}
    			
    			else {
    				int index = getIndex(v);
    				
    				double elementCount = 0;
    				
    				for (int i = 0; i < index; i++){
    					elementCount += buckets[i];
    				}
    				
    				double offset = v - min - index*width + 1;
    				
    				System.out.println(index);
    				elementCount += (offset/width)*buckets[index];
    				
    				
    				return elementCount/numTuples;
    				
    				
    			}
    			
    		case GREATER_THAN:
    			return 1 - estimateSelectivity(Predicate.Op.LESS_THAN_OR_EQ, v);
    		case GREATER_THAN_OR_EQ:
    			return 1 - estimateSelectivity(Predicate.Op.LESS_THAN, v);
    		case EQUALS:
       			if (v <= min){
    				return 0.0;
    			}
    			
    			else if (v >= max){
    		
    				return 0.0;
    			}
    			
    			else {

    				
    				int index = getIndex(v);
    				//System.out.println(index);
    				
    				double elementCount = buckets[index];
    				//System.out.println(width);
    				
    				//System.out.println(numTuples);
    				
    				return (((double) elementCount)/((double) width))/((double) numTuples);
    				
    				
    			}

    			
    		case NOT_EQUALS:
    			return 1 - estimateSelectivity(Predicate.Op.EQUALS, v);
    		case LIKE:
    			return estimateSelectivity(Predicate.Op.EQUALS, v);
    		default:
    			throw new RuntimeException("Shouldn't come here");
    		
    		
    	
    	}
    	

    }
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        // some code goes here
        return 1.0;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
    	
    	String result = "";
        for(int i = 0; i < buckets.length; i++){
        	
        	result += "(Bucket No " + i + " with " + buckets[i] + ") ";
        	
        	
        }
        
        return result;
    }
}

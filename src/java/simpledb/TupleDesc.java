package simpledb;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        public final Type fieldType;

        /**
         * The name of the field
         * */
        public final String fieldName;


        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        // some code goes here
        return null;
    }

    private static final long serialVersionUID = 1L;

    /**
     *  array of TD items
     */

    private final TDItem[] tdItems;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     *
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {

    	int numTypes = typeAr.length;
    	tdItems = new TDItem[numTypes];

    	for (int i = 0; i < numTypes; i++) {

    		tdItems[i] = new TDItem(typeAr[i], fieldAr[i]);
    	}
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     *
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {

    	int numTypes = typeAr.length;
    	tdItems = new TDItem[numTypes];

    	for (int i = 0; i < numTypes; i++) {
    		// TODO since no name is specified, just gonna pass empty string for now
    		tdItems[i] = new TDItem(typeAr[i], "");
    	}


    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {

        return tdItems.length;
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     *
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {

    	int numField = tdItems.length;

    	if (i == -1 || i > numField - 1){
    		throw new NoSuchElementException(String.valueOf(i) + " is not a valid field reference");
    	}

        TDItem associatedField = tdItems[i];

        return associatedField.fieldName;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     *
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
    	int numField = tdItems.length;

    	if (i == -1 || i > numField - 1){
    		throw new NoSuchElementException(String.valueOf(i) + " is not a valid field reference");
    	}

        TDItem associatedField = tdItems[i];

        return associatedField.fieldType;
    }

    /**
     * Find the index of the field with a given name.
     *
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {

    	int numField = tdItems.length;

    	int foundIndex = -1;

    	for (int i = 0; i < numField; i++){
    		if (tdItems[i].fieldName.equals(name)){
    			foundIndex = i;
    		}
    	}

    	if (foundIndex == -1){
    		throw new NoSuchElementException("No matching index was found");
    	}

        return foundIndex;
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // some code goes here
    	int size = 0;

    	for (int i = 0; i < this.numFields(); i++){
    		size += this.getFieldType(i).getLen();
    	}
        return size;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     *
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {

    	// TODO maybe more tests here???

    	int numField1 = td1.numFields();
    	int numField2 = td2.numFields();
    	int totalNumField = numField1 + numField2;

    	Type[] typeArray = new Type[totalNumField];
    	String[] fieldArray = new String[totalNumField];

    	for (int i = 0; i < numField1; ++i){

    		// TODO sketchy because might throw exception
    		typeArray[i] = td1.getFieldType(i);
    		fieldArray[i] = td1.getFieldName(i);
    	}

		for (int i = 0; i < numField2; ++i) {

			// TODO sketchy because might throw exception
			typeArray[i + numField1] = td2.getFieldType(i);
			fieldArray[i + numField1] = td2.getFieldName(i);
		}



        return new TupleDesc(typeArray, fieldArray);
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they have the same number of items
     * and if the i-th type in this TupleDesc is equal to the i-th type in o
     * for every i.
     *
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */

    public boolean equals(Object o) {

    	// if the object is not even a TupleDesc instance, return false
        if (!(o instanceof TupleDesc)){
        	return false;
        }

        TupleDesc that = (TupleDesc) o;

        int objectNumField = that.numFields();

        // checks if they have same length
        boolean check1 = objectNumField == this.numFields();

        // wanna return here, since it is obviously not equal
        if (!check1){
        	return false;
        }

        boolean check2 = true;
        for (int i = 0; i < this.numFields(); ++i){
			if ((this.getFieldType(i) != that.getFieldType(i)) || (this.getFieldType(i) != that.getFieldType(i))) {
               check2 = false;
			}

        }

        return check2;


    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     *
     * @return String describing this descriptor.
     */
    public String toString() {
        StringBuilder sb = new StringBuilder();

        for(int i = 0; i < this.numFields(); i++){

        	Type fieldType = this.getFieldType(i);
        	String fieldName = this.getFieldName(i);

        	// avoid exception
        	if (fieldName == null){
        		fieldName = "";
        	}

        	sb.append(fieldType.toString() + "(" + fieldName + ")");


        }
        return sb.toString();
    }
}

package simpledb;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    private int min, max;
    private int[] histgrams;
    private int bucketWidth;
    private int totalTups = 0;
    
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
    	this.max = max;
    	this.bucketWidth = (int) Math.ceil((max - min + 1.0) / buckets);
    	histgrams = new int[buckets];
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * 
     * @param v
     *            Value to add to the histogram. Must ensure min <= v <= max.
     */
    public void addValue(int v) {
        int bucketNo = (v - min) / bucketWidth;
    	histgrams[bucketNo]++;
    	totalTups += 1;
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
        if (totalTups == 0) {
            return 1.0;
        }
        
        double selectivity;
        double eqCount;
        double greaterCount;
        
        
        if (v < min) {
            eqCount = 0;
            greaterCount = totalTups;
        } else if (v > max) {
            eqCount = 0;
            greaterCount = 0;
        } else {
            int bucketNo = (v - min) / bucketWidth;
            // the last bucket may have smaller width
            int width;
            if (bucketNo == histgrams.length) {
                width = max - min - (histgrams.length - 1) * bucketWidth + 1;
            } else {
                width = bucketWidth;
            }
            eqCount = ((double) histgrams[bucketNo]) / width;
            greaterCount = 0;
            for (int i = bucketNo + 1; i < histgrams.length; i++) {
                greaterCount += histgrams[i];
            }
            greaterCount += eqCount
                    * (width - (v - (min + bucketNo * bucketWidth - 1)));
        }
        
        switch (op) {
        case EQUALS:
            selectivity = eqCount / totalTups;
            break;
        case GREATER_THAN:
            selectivity = greaterCount / totalTups;
            break;
        case GREATER_THAN_OR_EQ:
            selectivity = (greaterCount + eqCount)/ totalTups;
            break;
        case LESS_THAN:
            selectivity = 1 - (greaterCount + eqCount)/ totalTups;
            break;
        case LESS_THAN_OR_EQ:
            selectivity = 1 - greaterCount / totalTups;
            break;
        case NOT_EQUALS:
            selectivity = 1 - eqCount / totalTups;
            break;
        default:
            // LIKE
            selectivity = 1.0;
        }
        
        return selectivity;
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
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < histgrams.length; i++) {
            int left = Math.max(min, min + bucketWidth * i);
            int right = Math.min(max, min + bucketWidth * (i + 1) - 1);
            sb.append(String.format("[ %s, %s ] : %s%n", left, right, histgrams[i]));
        }
        return sb.toString();
    }
}

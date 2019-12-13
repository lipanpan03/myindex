package cn.edu.thu.dquality.back.utility;

public interface DQDataIterator {
    /**
     * get names of all columns
     */
    String[] getColsName();

    /**
     * get the index of the timestamp column
     */
    int getTimeColIndex();

    /**
     * get the index of all numeric columns
     */
    int[] getNumericColsIndex();

    /**
     * get the next row, or null if all rows has been scanned
     */
    String next();
}

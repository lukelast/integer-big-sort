package com.lukelast.bigsort;

import java.io.Closeable;
import java.io.IOException;

public interface IntegerStore extends Closeable
{
    /**
     * @param index The index of the starting integer to get. Index increments
     *            once for each integer.
     * @param length The number of integers to get.
     * @return The data fetched.
     * @throws IOException
     */
    int[] get( long index, int length ) throws IOException;

    /**
     * @param data Data to put to the data store.
     * @param index The starting integer index.
     * @throws IOException
     */
    void put( int[] data, long index ) throws IOException;
}
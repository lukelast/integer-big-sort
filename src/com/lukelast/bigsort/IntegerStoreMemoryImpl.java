package com.lukelast.bigsort;

import java.io.IOException;
import java.util.Arrays;

/**
 * An implementation of {@link IntegerStore} that keeps everything in RAM for
 * fast unit testing of other components.
 * @author Luke Last
 */
public final class IntegerStoreMemoryImpl implements IntegerStore
{
    private final int[] mData;

    public IntegerStoreMemoryImpl( int maxCapacity )
    {
        mData = new int[maxCapacity];
    }

    @Override
    public void close() throws IOException
    {
    }

    @Override
    public int[] get( long index, int length ) throws IOException
    {
        return Arrays.copyOfRange( mData, (int) index, (int) index + length );
    }

    @Override
    public void put( int[] data, long index ) throws IOException
    {

        System.arraycopy( data, 0, mData, (int) index, data.length );
    }
}
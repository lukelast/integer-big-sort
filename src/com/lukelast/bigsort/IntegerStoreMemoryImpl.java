package com.lukelast.bigsort;

import java.io.IOException;
import java.nio.IntBuffer;

public final class IntegerStoreMemoryImpl implements IntegerDataStore
{
    private final IntBuffer mBuffer;

    public IntegerStoreMemoryImpl( int maxCapacity )
    {
        mBuffer = IntBuffer.allocate( maxCapacity );
    }

    @Override
    public void close() throws IOException
    {
    }

    @Override
    public int[] get( long index, int length ) throws IOException
    {
        int[] data = new int[length];
        mBuffer.position( (int) index );
        mBuffer.get( data );
        return data;
    }

    @Override
    public void put( int[] data, long index ) throws IOException
    {
        mBuffer.position( (int) index );
        mBuffer.put( data );
    }
}
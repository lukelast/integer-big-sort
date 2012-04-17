package com.lukelast.bigsort;

import java.io.IOException;
import java.util.Arrays;

/**
 * Provides integer stream access to the packet based data store. A single
 * stream should only be used for reading or writing but not both at the same
 * time.
 * @author Luke Last
 */
public final class IntegerStream
{
    /**
     * The current index within the current chunk of data {@link #mDataChunk}.
     */
    private int mChunkIndex;
    /**
     * The size of each {@link #mDataChunk} cached.
     */
    private final int mChunkSize;
    private int[] mDataChunk;
    private final IntegerStore mDataStore;
    /**
     * The index within the data store that the current chunk starts at.
     */
    private long mDataStoreIndex;
    private final long mIndexLast;

    /**
     * @param dataStore The backing data store for this stream.
     * @param indexFirst The index position in the data store to start the
     *            stream.
     * @param length The length of the stream.
     * @param chunkSize The size in integers of the chunks to read and write at
     *            a time to the data store.
     */
    IntegerStream( IntegerStore dataStore, long indexFirst, long length, int chunkSize )
    {
        this.mDataStore = dataStore;
        this.mIndexLast = ( indexFirst + length ) - 1L;
        this.mDataStoreIndex = indexFirst;
        this.mChunkSize = chunkSize;
        this.mChunkIndex = Integer.MAX_VALUE;
    }

    private int findNextChunkSize()
    {
        return (int) Math.min( mChunkSize, ( mIndexLast - mDataStoreIndex ) + 1L );
    }

    /**
     * Write any data pending in the cache to the data store.
     * @throws IOException
     */
    public void flush() throws IOException
    {
        if ( mDataChunk != null && 0 < mChunkIndex )
        {
            int[] data;
            if ( mDataChunk.length <= mChunkIndex )
                data = mDataChunk;
            else
                data = Arrays.copyOf( mDataChunk, mChunkIndex );
            mDataStore.put( data, mDataStoreIndex );
            mDataStoreIndex += data.length;
        }
        mChunkIndex = 0;
    }

    /**
     * Get the current value and then move to the next index position.
     * @return The current value.
     * @throws IOException
     */
    public int getNext() throws IOException
    {
        this.maybeLoadNextChunk();
        return mDataChunk[mChunkIndex++];
    }

    public boolean hasNext()
    {
        return mChunkIndex < mDataChunk.length || mDataStoreIndex <= mIndexLast;
    }

    private void maybeLoadNextChunk() throws IOException
    {
        if ( mChunkSize <= mChunkIndex )
        {
            this.mDataChunk = mDataStore.get( mDataStoreIndex, findNextChunkSize() );
            this.mDataStoreIndex += mDataChunk.length;
            this.mChunkIndex = 0;
        }
    }

    /**
     * @return The value at the current index position without incrementing.
     *         Meaning repeated calls always return the same value until
     *         {@link #getNext()} is called.
     * @throws IOException
     */
    public int peekNext() throws IOException
    {
        this.maybeLoadNextChunk();
        return mDataChunk[mChunkIndex];
    }

    /**
     * Write this value and increment the local index to the next position.
     * @param value 32 bit value to write.
     * @throws IOException
     */
    public void write( int value ) throws IOException
    {
        if ( mDataChunk == null || mDataChunk.length <= mChunkIndex )
        {
            this.flush();
            final int chunkSize = findNextChunkSize();
            if ( mDataChunk == null || mDataChunk.length != chunkSize )
                mDataChunk = new int[chunkSize];
        }
        mDataChunk[mChunkIndex++] = value;
    }
}
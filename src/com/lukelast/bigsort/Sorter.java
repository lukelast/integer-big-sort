package com.lukelast.bigsort;

import java.io.IOException;
import java.util.Arrays;
import java.util.PriorityQueue;

public class Sorter
{
    protected final int mChunkCount;
    protected final long mChunkSize;
    protected final IntegerDataStore mFinishStore;
    protected final IntegerDataStore mStartStore;
    protected final long mTotalIntegerCount;

    public Sorter( IntegerDataStore integerStore, IntegerDataStore finishStore, long chunkSize,
            int totalChunks )
    {
        this.mStartStore = integerStore;
        this.mFinishStore = finishStore;
        this.mChunkSize = chunkSize;
        this.mChunkCount = totalChunks;
        this.mTotalIntegerCount = chunkSize * totalChunks;
    }

    protected int calculateMergeChunkSize()
    {
        if ( mChunkSize <= mChunkCount )
            return (int) mChunkSize;
        else
            return (int) ( mChunkSize / mChunkCount );
    }

    public void doChunkSort() throws IOException
    {
        for ( int chunkCount = 0; chunkCount < mChunkCount; chunkCount++ )
        {
            int[] dataChunk = mStartStore.get( chunkCount * mChunkSize, (int) mChunkSize );
            Arrays.sort( dataChunk );
            mStartStore.put( dataChunk, chunkCount * mChunkSize );
        }
    }

    public void doMergeSort() throws IOException
    {
        PriorityQueue<ChunkCursor> queue = new PriorityQueue<ChunkCursor>( mChunkCount,
                                                                           new CursorComparator() );

        final int chunkSize = this.calculateMergeChunkSize();
        for ( int chunkCount = 0; chunkCount < mChunkCount; chunkCount++ )
        {
            queue.add( new ChunkCursor( mStartStore, chunkCount * mChunkSize, mChunkSize, chunkSize ) );
        }

        final ChunkCursor resultStream = new ChunkCursor( mFinishStore,
                                                          0,
                                                          mTotalIntegerCount,
                                                          (int) mChunkSize );

        while ( !queue.isEmpty() )
        {
            // First key is the lowest value.
            final ChunkCursor smallest = queue.remove();
            resultStream.write( smallest.getNext() );
            // If the cursor is not empty add it back to be re-sorted in the queue.
            if ( smallest.hasNext() )
                queue.add( smallest );
        }
        resultStream.flush();
    }

    public boolean doVerifyResult() throws IOException
    {
        ChunkCursor resultRead = new ChunkCursor( mFinishStore,
                                                  0,
                                                  mTotalIntegerCount,
                                                  (int) mChunkSize );
        int lastValue = Integer.MIN_VALUE;
        for ( long count = 0; count < mTotalIntegerCount; count++ )
        {
            final int value = resultRead.getNext();
            if ( value < lastValue )
                return false;
            lastValue = value;
        }
        return true;
    }

    public void fillWithRandomIntegers() throws IOException
    {
        Utils.fillWithRandomIntegers( mStartStore, 0, mTotalIntegerCount );
    }

}

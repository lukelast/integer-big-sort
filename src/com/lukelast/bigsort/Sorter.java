package com.lukelast.bigsort;

import java.io.IOException;
import java.util.Arrays;
import java.util.PriorityQueue;
import java.util.Random;

/**
 * Implement code which can sort 10 billion integers, assuming a constraint on
 * memory (i.e. that the full set will not fit into memory at once.) Document
 * any assumptions you make in order to do so. Write a unit test to exercise the
 * code.
 * @author Luke Last
 */
public class Sorter
{
    protected final int mChunkCount;
    protected final long mChunkSize;
    protected final IntegerStore mFinishStore;
    private long mStageStartTime;
    protected final IntegerStore mStartStore;
    private long mStartTime;
    protected final long mTotalIntegerCount;

    public Sorter( IntegerStore integerStore, IntegerStore finishStore, long chunkSize,
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

    /**
     * @return {@code true} if the verify stage succeeded.
     * @throws Exception
     */
    public boolean doAllStages() throws Exception
    {
        this.doFillWithRandomIntegers();
        this.doChunkSort();
        this.doMergeSort();
        return this.doVerifyResult();
    }

    public void doChunkSort() throws IOException
    {
        print( "Starting Stage-2 doChunkSort" );
        for ( int chunkCount = 0; chunkCount < mChunkCount; chunkCount++ )
        {
            int[] dataChunk = mStartStore.get( chunkCount * mChunkSize, (int) mChunkSize );
            Arrays.sort( dataChunk );
            mStartStore.put( dataChunk, chunkCount * mChunkSize );
        }
        print( "Finished Stage-2 doChunkSort" );
    }

    public void doFillWithRandomIntegers() throws IOException
    {
        System.out.println( mTotalIntegerCount / ( 1000.0 * 1000.0 ) + " million" );
        print( "Starting Stage-1 fillWithRandomIntegers" );
        fillWithRandomIntegers( 0, mTotalIntegerCount );
        print( "Finished Stage-1 fillWithRandomIntegers" );
    }

    public void doMergeSort() throws IOException
    {
        print( "Starting Stage-3 doMergeSort" );
        PriorityQueue<IntegerStream> queue = new PriorityQueue<IntegerStream>( mChunkCount,
                                                                               new IntegerStreamComparator() );

        final int chunkSize = this.calculateMergeChunkSize();
        for ( int chunkCount = 0; chunkCount < mChunkCount; chunkCount++ )
        {
            queue.add( new IntegerStream( mStartStore,
                                          chunkCount * mChunkSize,
                                          mChunkSize,
                                          chunkSize ) );
        }

        final IntegerStream resultStream = new IntegerStream( mFinishStore,
                                                              0,
                                                              mTotalIntegerCount,
                                                              (int) mChunkSize );

        while ( !queue.isEmpty() )
        {
            // First key is the lowest value.
            final IntegerStream smallest = queue.remove();
            resultStream.write( smallest.getNext() );
            // If the cursor is not empty add it back to be re-sorted in the queue.
            if ( smallest.hasNext() )
                queue.add( smallest );
        }
        resultStream.flush();
        print( "Finished Stage-3 doMergeSort" );
    }

    public boolean doVerifyResult() throws IOException
    {
        print( "Starting Stage-4 doVerifyResult" );
        IntegerStream resultRead = new IntegerStream( mFinishStore,
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
        print( "Finished Stage-4 doVerifyResult" );
        return true;
    }

    protected final void fillWithRandomIntegers( long start, long size ) throws IOException
    {
        final Random rand = new Random();
        IntegerStream writer = new IntegerStream( mStartStore, start, size, (int) mChunkSize );
        for ( long count = 0; count < size; count++ )
        {
            writer.write( rand.nextInt() );
        }
        writer.flush();
    }

    protected void print( String message )
    {
        final long now = System.currentTimeMillis();
        if ( mStartTime == 0 )
            mStartTime = now;

        final StringBuilder sb = new StringBuilder();
        sb.append( ( now - mStartTime ) / 1000 ).append( " seconds --- " );
        if ( mStageStartTime == 0 )
        {
            mStageStartTime = now;

        }
        else
        {
            sb.append( " Stage took " )
                .append( ( now - mStageStartTime ) / 1000 )
                .append( " seconds --- " );
            mStageStartTime = 0;
        }
        sb.append( message );
        System.out.println( sb.toString() );
    }

}

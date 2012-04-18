package com.lukelast.bigsort;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public final class ConcurrentSorter extends Sorter
{
    private final ExecutorService mExecutor;

    public ConcurrentSorter( ExecutorService executor, IntegerStore integerStore,
            IntegerStore finishStore, long chunkSize, int totalChunks )
    {
        super( integerStore, finishStore, chunkSize, totalChunks );
        this.mExecutor = executor;
    }

    public ConcurrentSorter( IntegerStore integerStore, IntegerStore finishStore, long chunkSize,
            int totalChunks )
    {
        super( integerStore, finishStore, chunkSize, totalChunks );
        mExecutor = Executors.newFixedThreadPool( 4 );
    }

    public void close()
    {
        mExecutor.shutdown();
    }

    @Override
    protected void doChunkSortImpl() throws Exception
    {
        final List<Future<?>> futures = new ArrayList<Future<?>>();
        for ( int chunkCount = 0; chunkCount < mChunkCount; chunkCount++ )
        {
            final long startIndex = chunkCount * mChunkSize;
            futures.add( mExecutor.submit( new Runnable()
            {
                @Override
                public void run()
                {
                    try
                    {
                        sortChunk( startIndex );
                    }
                    catch ( IOException ex )
                    {
                        ex.printStackTrace();
                    }
                }
            } ) );
        }

        for ( Future<?> future : futures )
        {
            future.get();
        }
    }

    @Override
    protected void doFillWithRandomIntegersImpl() throws Exception
    {
        final List<Future<?>> futures = new ArrayList<Future<?>>();
        final long integersPerRun;
        // Test if it is divisible by 8.
        if ( mTotalIntegerCount == ( mTotalIntegerCount & ( ~0b111 ) ) )
            integersPerRun = ( mTotalIntegerCount / 8 );
        else
            integersPerRun = mTotalIntegerCount;

        for ( long index = 0; index < mTotalIntegerCount; index += integersPerRun )
        {
            final long startIndex = index;
            futures.add( mExecutor.submit( new Runnable()
            {
                @Override
                public void run()
                {
                    try
                    {
                        fillWithRandomIntegers( startIndex, integersPerRun );
                    }
                    catch ( Exception e )
                    {
                        e.printStackTrace();
                    }
                }
            } ) );
        }

        for ( Future<?> future : futures )
        {
            future.get();
        }
    }

    @Override
    protected void doMergeSortImpl( IntegerStream resultStream ) throws Exception
    {
        super.doMergeSortImpl( new ConcurrentIntegerStream( mExecutor,
                                                            mFinishStore,
                                                            0,
                                                            mTotalIntegerCount,
                                                            (int) mChunkSize ) );
    }
}

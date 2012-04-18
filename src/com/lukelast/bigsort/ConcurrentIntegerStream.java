package com.lukelast.bigsort;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

public final class ConcurrentIntegerStream extends IntegerStream
{
    private final ExecutorService mExecutor;
    private final Collection<Future<?>> mPending = new ConcurrentLinkedQueue<Future<?>>();

    ConcurrentIntegerStream( ExecutorService executor, IntegerStore dataStore, long indexFirst,
            long length, int chunkSize )
    {
        super( dataStore, indexFirst, length, chunkSize );
        this.mExecutor = executor;
    }

    @Override
    public void flush() throws IOException, InterruptedException, ExecutionException
    {
        super.flush();
        for ( Future<?> pending : mPending )
        {
            pending.get();
        }
    }

    @Override
    protected void writeBuffer() throws IOException
    {
        final int[] data = mDataChunk;
        final int index = mChunkIndex;
        final long dataStoreIndex = mDataStoreIndex;
        if ( data != null && 0 < index )
        {
            mPending.add( mExecutor.submit( new Runnable()
            {
                @Override
                public void run()
                {
                    try
                    {
                        mDataStore.put( ( data.length <= index ) ? data : Arrays.copyOf( data,
                                                                                         index ),
                                        dataStoreIndex );
                    }
                    catch ( IOException ex )
                    {
                        ex.printStackTrace();
                    }
                    for ( Iterator<Future<?>> iterator = mPending.iterator(); iterator.hasNext(); )
                    {
                        if ( iterator.next().isDone() )
                            iterator.remove();
                    }
                }
            } ) );
            mDataStoreIndex += mChunkIndex;
        }
        mDataChunk = new int[findNextChunkSize()];

        mChunkIndex = 0;
    }
}
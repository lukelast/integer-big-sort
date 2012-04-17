package com.lukelast.bigsort;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public final class ConcurrentSorter extends Sorter
{
    private final ExecutorService mExecutor = Executors.newCachedThreadPool();

    public ConcurrentSorter( IntegerStore integerStore, IntegerStore finishStore, long chunkSize,
            int totalChunks )
    {
        super( integerStore, finishStore, chunkSize, totalChunks );
    }

    @Override
    public void doFillWithRandomIntegers() throws IOException
    {
        final List<Future<?>> futures = new ArrayList<Future<?>>();
        final long integersPerRun = ( mTotalIntegerCount / 8 );
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
                    catch ( IOException e )
                    {
                        e.printStackTrace();
                    }
                }
            } ) );
        }

        for ( Future<?> future : futures )
        {
            try
            {
                future.get();
            }
            catch ( Exception ex )
            {
                ex.printStackTrace();
            }
        }
    }
}

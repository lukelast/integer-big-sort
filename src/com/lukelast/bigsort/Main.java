package com.lukelast.bigsort;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.PriorityQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Implement code which can sort 10 billion integers, assuming a constraint on
 * memory (i.e. that the full set will not fit into memory at once.) Document
 * any assumptions you make in order to do so. Write a unit test to exercise the
 * code.
 * @author Luke Last
 */
public final class Main
{
    /**
     * @param args
     */
    public static void main( String[] args )
    {
        try
        {
            final long startTime = System.currentTimeMillis();

            final IntegerDataStore dataStore = new IntegerDataStoreFileImpl( "start" );
            final long chunkSize = 1000 * 1000 * 10;
            final int totalChunks = 10;
            final long totalIntegerCount = chunkSize * (long) totalChunks;

            print( startTime, "Starting with " + totalIntegerCount / ( 1000 * 1000.0 ) + "m" );

            ExecutorService executor = Executors.newCachedThreadPool();
            final List<Future<?>> futures = new ArrayList<Future<?>>();

            for ( long index = 0; index < totalIntegerCount; index += ( totalIntegerCount / 8 ) )
            {
                final long startIndex = index;
                futures.add( executor.submit( new Runnable()
                {
                    @Override
                    public void run()
                    {
                        try
                        {
                            Utils.fillWithRandomIntegers( dataStore,
                                                          startIndex,
                                                          ( totalIntegerCount / 8 ) );
                        }
                        catch ( IOException e )
                        {
                            e.printStackTrace();
                        }
                    }
                } ) );
            }

            // Utils.fillWithRandomIntegers( dataStore, totalIntegerCount );
            for ( Future<?> future : futures )
            {
                future.get();
            }
            print( startTime, "Finished writing random integers" );

            for ( int chunkCount = 0; chunkCount < totalChunks; chunkCount++ )
            {
                int[] dataChunk = dataStore.get( chunkCount * chunkSize, (int) chunkSize );
                Arrays.sort( dataChunk );
                dataStore.put( dataChunk, chunkCount * chunkSize );
            }

            print( startTime, "Finished sorting chunks" );

            final List<ChunkCursor> chunkCursors = new ArrayList<ChunkCursor>( totalChunks );
            for ( int chunkCount = 0; chunkCount < totalChunks; chunkCount++ )
            {
                chunkCursors.add( new ChunkCursor( dataStore,
                                                   chunkCount * chunkSize,
                                                   chunkSize,
                                                   (int) ( chunkSize / totalChunks ) ) );
            }

            PriorityQueue<ChunkCursor> queue = new PriorityQueue<ChunkCursor>( totalChunks,
                                                                               new CursorComparator() );
            queue.addAll( chunkCursors );

            final IntegerDataStore resultDataStore = new IntegerDataStoreFileImpl( "result" );
            final ChunkCursor resultStream = new ChunkCursor( resultDataStore,
                                                              0,
                                                              totalIntegerCount,
                                                              (int) chunkSize );

            print( startTime, "Starting merge" );

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

            ChunkCursor resultRead = new ChunkCursor( resultDataStore,
                                                      0,
                                                      totalIntegerCount,
                                                      (int) chunkSize );
            print( startTime, "Starting verify" );
            int lastValue = Integer.MIN_VALUE;
            for ( long count = 0; count < totalIntegerCount; count++ )
            {
                final int value = resultRead.getNext();
                if ( value < lastValue )
                    throw new IllegalStateException();
                lastValue = value;
            }
            print( startTime, "YAY!" );
        }
        catch ( Exception e )
        {
            e.printStackTrace();
        }
    }

    private static void print( long startTime, String message )
    {
        final long elapsed = ( System.currentTimeMillis() - startTime ) / 1000;
        System.out.println( elapsed + " " + message );
    }
}
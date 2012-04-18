package com.lukelast.bigsort;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

public class ConcurrentSorterTest
{
    private static final ExecutorService EXECUTOR = Executors.newFixedThreadPool( 4 );

    private static Sorter newSorter( long chunkSize, long chunks ) throws FileNotFoundException
    {
        return new ConcurrentSorter( new IntegerStoreFileImpl( new File( SorterTest.TEST_DIRECTORY,
                                                                         "start.dat" ) ),
                                     new IntegerStoreFileImpl( new File( SorterTest.TEST_DIRECTORY,
                                                                         "finish.dat" ) ),
                                     chunkSize,
                                     (int) chunks );
    }

    @BeforeClass
    public static void setUpBeforeClass() throws Exception
    {
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception
    {
        EXECUTOR.shutdown();
    }

    @Before
    public void setUp() throws Exception
    {
    }

    @After
    public void tearDown() throws Exception
    {
    }

    /**
     * @throws Exception
     */
    @Test
    @Ignore
    public void test100Million() throws Exception
    {
        final Sorter sorter = newSorter( 1000 * 1000 * 10, 10 );
        assertTrue( sorter.doAllStages() );
    }

    @Test
    @Ignore
    public void test10Billion() throws Exception
    {
        assertTrue( newSorter( 1000 * 1000 * 5, 2000 ).doAllStages() );
    }

    @Test
    public void test10mInMemory() throws Exception
    {
        final int chunkSize = 1000 * 100;
        final int chunks = 100;
        final int size = chunks * chunkSize;
        final Sorter sorter = new ConcurrentSorter( new IntegerStoreMemoryImpl( size ),
                                                    new IntegerStoreMemoryImpl( size ),
                                                    chunkSize,
                                                    chunks );

        assertTrue( sorter.doAllStages() );
    }

    /**
     * Took 592 seconds on Thinkpad.
     * @throws Exception
     */
    @Test
    @Ignore
    public void test1Billion() throws Exception
    {
        assertTrue( newSorter( 1000 * 1000 * 5, 200 ).doAllStages() );
    }

    @Test
    public void testSortInMemoryDiffChunks() throws Exception
    {
        for ( int chunks = 1; chunks < 60; chunks++ )
        {
            for ( int chunkSize = 1; chunkSize < 60; chunkSize++ )
            {
                final int size = chunks * chunkSize;
                final IntegerStore start = new IntegerStoreMemoryImpl( size );
                final IntegerStore finish = new IntegerStoreMemoryImpl( size );
                final Sorter sorter = new ConcurrentSorter( EXECUTOR,
                                                            start,
                                                            finish,
                                                            chunkSize,
                                                            chunks );
                assertTrue( "Chunks: " + chunks + " ChunkSize: " + chunkSize, sorter.doAllStages() );
            }
        }
    }
}
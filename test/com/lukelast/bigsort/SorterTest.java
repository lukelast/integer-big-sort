package com.lukelast.bigsort;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileNotFoundException;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

public class SorterTest
{
    private static final File TEST_DIRECTORY = new File( "." );

    private static Sorter newSorter( long chunkSize, long chunks ) throws FileNotFoundException
    {
        return new Sorter( new IntegerStoreFileImpl( new File( TEST_DIRECTORY, "start.dat" ) ),
                           new IntegerStoreFileImpl( new File( TEST_DIRECTORY, "finish.dat" ) ),
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
     * 727 seconds.
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
        final Sorter sorter = newSorter( 1000 * 1000 * 10, 1000 );
        sorter.doFillWithRandomIntegers();
        sorter.doChunkSort();
        sorter.doMergeSort();
        assertTrue( sorter.doVerifyResult() );
    }

    /**
     * 727 seconds.
     * @throws Exception
     */
    @Test
    @Ignore
    public void test1Billion() throws Exception
    {
        final Sorter sorter = newSorter( 1000 * 1000 * 10, 100 );
        sorter.doFillWithRandomIntegers();
        sorter.doChunkSort();
        sorter.doMergeSort();
        assertTrue( sorter.doVerifyResult() );
    }

    @Test
    public void testSort1mInMemory() throws Exception
    {
        final int chunkSize = 1000 * 10;
        final int chunks = 100;
        final int size = chunks * chunkSize;
        final IntegerStore start = new IntegerStoreMemoryImpl( size );
        final IntegerStore finish = new IntegerStoreMemoryImpl( size );
        final Sorter sorter = new Sorter( start, finish, chunkSize, chunks );

        sorter.doFillWithRandomIntegers();
        sorter.doChunkSort();
        sorter.doMergeSort();
        assertTrue( sorter.doVerifyResult() );
    }

    @Test
    public void testSortInMemoryDiffChunks() throws Exception
    {
        for ( int chunks = 1; chunks < 100; chunks++ )
        {
            for ( int chunkSize = 1; chunkSize < 100; chunkSize++ )
            {
                final int size = chunks * chunkSize;
                final IntegerStore start = new IntegerStoreMemoryImpl( size );
                final IntegerStore finish = new IntegerStoreMemoryImpl( size );
                final Sorter sorter = new Sorter( start, finish, chunkSize, chunks );

                sorter.doFillWithRandomIntegers();
                sorter.doChunkSort();
                sorter.doMergeSort();
                assertTrue( "Chunks: " + chunks + " ChunkSize: " + chunkSize,
                            sorter.doVerifyResult() );
            }
        }
    }

}

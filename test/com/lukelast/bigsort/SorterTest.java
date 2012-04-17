package com.lukelast.bigsort;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class SorterTest
{

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

    @Test
    public void test10Billion() throws Exception
    {
        final Sorter sorter = new Sorter( new IntegerDataStoreFileImpl( "start" ),
                                          new IntegerDataStoreFileImpl( "finish" ),
                                          1000 * 1000 * 10,
                                          1000 );
        sorter.fillWithRandomIntegers();
        sorter.doChunkSort();
        sorter.doMergeSort();
        assertTrue( sorter.doVerifyResult() );
    }

    /**
     * 727 seconds.
     * @throws Exception
     */
    @Test
    public void test1Billion() throws Exception
    {
        final Sorter sorter = new Sorter( new IntegerDataStoreFileImpl( "start" ),
                                          new IntegerDataStoreFileImpl( "finish" ),
                                          1000 * 1000 * 10,
                                          100 );
        sorter.fillWithRandomIntegers();
        sorter.doChunkSort();
        sorter.doMergeSort();
        assertTrue( sorter.doVerifyResult() );
    }

    @Test
    public void testDoMergeSort()
    {
        fail( "Not yet implemented" );
    }

    @Test
    public void testDoVerifyResult()
    {
        fail( "Not yet implemented" );
    }

    @Test
    public void testSort1mInMemory() throws Exception
    {
        final int chunkSize = 1000 * 10;
        final int chunks = 100;
        final int size = chunks * chunkSize;
        final IntegerDataStore start = new IntegerStoreMemoryImpl( size );
        final IntegerDataStore finish = new IntegerStoreMemoryImpl( size );
        final Sorter sorter = new Sorter( start, finish, chunkSize, chunks );

        sorter.fillWithRandomIntegers();
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
                final IntegerDataStore start = new IntegerStoreMemoryImpl( size );
                final IntegerDataStore finish = new IntegerStoreMemoryImpl( size );
                final Sorter sorter = new Sorter( start, finish, chunkSize, chunks );

                sorter.fillWithRandomIntegers();
                sorter.doChunkSort();
                sorter.doMergeSort();
                assertTrue( "Chunks: " + chunks + " ChunkSize: " + chunkSize,
                            sorter.doVerifyResult() );
            }
        }
    }

}

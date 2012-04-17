package com.lukelast.bigsort;

import static org.junit.Assert.assertArrayEquals;

import java.util.Arrays;
import java.util.Random;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Unit tests for the {@link IntegerDataStoreFileImpl} class.
 * @author Luke Last
 */
public class IntegerDataStoreFileImplTest
{
    private static final int[] ORDERED = new int[100];

    private static final int[] RANDOM = new int[100];

    private static final int[] SAME = new int[100];

    /**
     * @throws java.lang.Exception
     */
    @BeforeClass
    public static void setUpBeforeClass() throws Exception
    {
        for ( int index = 0; index < ORDERED.length; index++ )
        {
            ORDERED[index] = index;
        }
        Random random = new Random();
        for ( int index = 0; index < RANDOM.length; index++ )
        {
            ORDERED[index] = random.nextInt();
        }
        Arrays.fill( SAME, 0xABC );
    }

    /**
     * @throws java.lang.Exception
     */
    @AfterClass
    public static void tearDownAfterClass() throws Exception
    {
    }
    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception
    {
    }
    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception
    {
    }

    /**
     * Test a bunch of puts and gets.
     */
    @Test
    public void testSmall1() throws Exception
    {
        final IntegerDataStoreFileImpl dataStore = new IntegerDataStoreFileImpl( "testSmall1" );

        for ( int index = 0; index < 100; index++ )
        {
            dataStore.put( ORDERED, index );
            final int[] readData = dataStore.get( index, ORDERED.length );
            assertArrayEquals( ORDERED, readData );
        }
    }

    /**
     * Test putting 2 chunks of data and making sure the 2nd one doesn't corrupt
     * the 1st one.
     */
    @Test
    public void testSmall2() throws Exception
    {
        final IntegerDataStoreFileImpl dataStore = new IntegerDataStoreFileImpl( "testSmall2" );
        dataStore.put( ORDERED, 0 );
        dataStore.put( RANDOM, ORDERED.length );
        final int[] readData = dataStore.get( 0, ORDERED.length );
        assertArrayEquals( ORDERED, readData );
    }
}
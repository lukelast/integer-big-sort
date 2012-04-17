package com.lukelast.bigsort;

import static org.junit.Assert.assertArrayEquals;

import java.util.Arrays;
import java.util.Random;

import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Unit tests for the {@link IntegerStoreFileImpl} class.
 * @author Luke Last
 */
public class IntegerStoreFileImplTest
{
    private static final int[] ORDERED = new int[100];
    private static final int[] RANDOM = new int[100];
    private static final int[] SAME = new int[100];

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
            RANDOM[index] = random.nextInt();
        }
        Arrays.fill( SAME, 0xABC );
    }

    /**
     * Test a bunch of puts and gets.
     */
    @Test
    public void testSmall1() throws Exception
    {
        final IntegerStoreFileImpl dataStore = new IntegerStoreFileImpl( "testSmall1" );

        for ( int index = 0; index < 100; index++ )
        {
            dataStore.put( ORDERED, index );
            final int[] readData = dataStore.get( index, ORDERED.length );
            assertArrayEquals( ORDERED, readData );
        }
        dataStore.close();
    }

    /**
     * Test putting 2 chunks of data and making sure the 2nd one doesn't corrupt
     * the 1st one.
     */
    @Test
    public void testSmall2() throws Exception
    {
        final IntegerStoreFileImpl dataStore = new IntegerStoreFileImpl( "testSmall2" );
        dataStore.put( ORDERED, 0 );
        dataStore.put( RANDOM, ORDERED.length );
        final int[] readData = dataStore.get( 0, ORDERED.length );
        assertArrayEquals( ORDERED, readData );
        dataStore.close();
    }
}
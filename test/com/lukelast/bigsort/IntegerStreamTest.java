package com.lukelast.bigsort;

import static org.junit.Assert.assertEquals;

import java.util.Random;

import org.junit.Before;
import org.junit.Test;

public class IntegerStreamTest
{
    private static final int DATA_SIZE = 1024 * 128;
    private int[] data = new int[DATA_SIZE];
    private final Random random = new Random();

    @Before
    public void setUp() throws Exception
    {
        for ( int index = 0; index < data.length; index++ )
        {
            data[index] = random.nextInt();
        }
    }

    @Test
    public void testWriteRead() throws Exception
    {
        for ( int run = 0; run < 1000; run++ )
        {
            final IntegerStoreMemoryImpl store = new IntegerStoreMemoryImpl( DATA_SIZE );
            final IntegerStream writer = new IntegerStream( store,
                                                        0,
                                                        DATA_SIZE,
                                                        random.nextInt( DATA_SIZE ) + 1 );
            for ( int index = 0; index < data.length; index++ )
            {
                writer.write( data[index] );
            }
            writer.flush();

            IntegerStream reader = new IntegerStream( store,
                                                  0,
                                                  DATA_SIZE,
                                                  random.nextInt( DATA_SIZE ) + 1 );
            for ( int index = 0; index < data.length; index++ )
            {
                assertEquals( data[index], reader.getNext() );
            }
        }
    }
}
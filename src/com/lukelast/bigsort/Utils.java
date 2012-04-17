package com.lukelast.bigsort;

import java.io.IOException;
import java.util.Random;

final class Utils
{
    public static void fillWithRandomIntegers( final IntegerDataStore store, final long start,
                                               final long size ) throws IOException
    {
        final Random rand = new Random();
        final ChunkCursor writerCursor = new ChunkCursor( store, start, size, 1024 * 1024 );
        for ( long count = 0; count < size; count++ )
        {
            writerCursor.write( rand.nextInt() );
        }
        writerCursor.flush();
    }

    private Utils()
    {
    }
}
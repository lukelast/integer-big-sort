package com.lukelast.bigsort;

import java.io.IOException;
import java.util.Comparator;

public final class CursorComparator implements Comparator<ChunkCursor>
{
    @Override
    public int compare( ChunkCursor first, ChunkCursor second )
    {
        try
        {
            final int val1 = first.peekNext();
            final int val2 = second.peekNext();
            if ( val1 == val2 )
                return first.hashCode() - second.hashCode();
            else if ( val1 < val2 )
                return -1;
            else
                return 1;
        }
        catch ( IOException e )
        {
            e.printStackTrace();
            throw new IllegalStateException();
        }
    }
}
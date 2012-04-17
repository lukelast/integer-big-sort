package com.lukelast.bigsort;

import java.io.IOException;
import java.util.Comparator;

/**
 * Compares 2 {@link IntegerStream}'s using their {@link IntegerStream#peekNext()}
 * integer value. We don't want equal values to compare as equal though, so in
 * that case we compare the object ID's.
 * @author Luke Last
 */
public final class IntegerStreamComparator implements Comparator<IntegerStream>
{
    @Override
    public int compare( IntegerStream first, IntegerStream second )
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
        catch ( IOException ex )
        {
            ex.printStackTrace();
            throw new IllegalStateException( ex );
        }
    }
}
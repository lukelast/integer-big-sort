package com.lukelast.bigsort;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;

public class IntegerDataStoreFileImpl implements IntegerDataStore
{
    private static final String DEFAULT_DIRECTORY = "C:\\test";
    private final File mFile;
    private final FileChannel mFileChannel;
    private final RandomAccessFile mRandomAccessFile;

    public IntegerDataStoreFileImpl( File file ) throws FileNotFoundException
    {
        this.mFile = file;
        new File( mFile, ".." ).mkdirs();
        mRandomAccessFile = new RandomAccessFile( mFile, "rw" );
        this.mFileChannel = mRandomAccessFile.getChannel();
    }

    public IntegerDataStoreFileImpl( String fileName ) throws FileNotFoundException
    {
        this( new File( DEFAULT_DIRECTORY, fileName + ".dat" ) );
    }

    @Override
    public void close() throws IOException
    {
        mRandomAccessFile.close();
        mFileChannel.close();
    }

    @Override
    public int[] get( long index, int length ) throws IOException
    {
        if ( index < 0 || length <= 0 )
            throw new IllegalArgumentException();

        final ByteBuffer buffer = mFileChannel.map( MapMode.READ_ONLY, index * 4, length * 4 );

        int[] data = new int[length];
        for ( int i = 0; i < data.length; i++ )
        {
            data[i] = buffer.getInt();
        }
        return data;
    }

    @Override
    public void put( final int[] data, final long index ) throws IOException
    {
        if ( data == null || data.length == 0 || index < 0 )
            throw new IllegalArgumentException();

        final int totalBytes = data.length * 4;

        final ByteBuffer buffer = ByteBuffer.allocate( totalBytes );
        for ( int i = 0; i < data.length; i++ )
        {
            buffer.putInt( data[i] );
        }
        buffer.flip();
        final long written = mFileChannel.write( buffer, index * 4 );

        //        long written = mFileChannel.transferFrom( new ReadableByteChannel()
        //        {
        //            private int index = 0;
        //
        //            @Override
        //            public boolean isOpen()
        //            {
        //                return true;
        //            }
        //
        //            @Override
        //            public void close() throws IOException
        //            {
        //            }
        //
        //            @Override
        //            public int read( ByteBuffer dst ) throws IOException
        //            {
        //                final int startIndex = index;
        //                while ( dst.hasRemaining() && index < data.length )
        //                {
        //                    dst.putInt( data[index++] );
        //                }
        //                return index - startIndex;
        //            }
        //        }, index * 4, totalBytes );

        if ( written != totalBytes )
            throw new IOException( "Failed to write all data" );
    }
}
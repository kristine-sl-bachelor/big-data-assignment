package xml;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.log4j.Logger;

import java.io.IOException;

public class XmlRecordReader extends RecordReader< LongWritable, Text > {

    private byte[] startTag;
    private byte[] endTag;

    private byte[] childStartTag;
    private byte[] childEndTag;

    private FSDataInputStream inputStream;
    private DataOutputBuffer buffer;

    private LongWritable key;
    private Text value;

    private long start;
    private long end;

    private static Logger LOG = Logger.getLogger( XmlRecordReader.class );

    @Override
    public void initialize( InputSplit split, TaskAttemptContext context ) throws IOException, InterruptedException {

        LOG.info( "Hello?" );
        Configuration config = context.getConfiguration();

        startTag = config.get( "fileStartTag" ).getBytes( "utf-8" );
        endTag = config.get( "fileEndTag" ).getBytes( "utf-8" );

        childStartTag = config.get( "childStartTag" ).getBytes( "utf-8" );
        childEndTag = config.get( "childEndTag" ).getBytes( "utf-8" );

        FileSplit fileSplit = ( FileSplit ) split;

        start = fileSplit.getStart();
        end = start + fileSplit.getLength();
        Path file = fileSplit.getPath();

        FileSystem fileSystem = file.getFileSystem( config );
        buffer = new DataOutputBuffer();

        inputStream = fileSystem.open( file );
        inputStream.seek( start );

        key = new LongWritable();
        value = new Text();
    }

    private int temp = 0;

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {

        LOG.info( "Starting search" );

        if ( findTag( startTag, false ) && !findTag( endTag, false ) ) {

            LOG.info( "Start tag found" );

            if ( findTag( childStartTag, true ) ) {

                buffer.write( childStartTag );

                if ( findTag( childEndTag, true ) ) {

                    key.set( inputStream.getPos() );
                    value.set( buffer.getData(), 0, buffer.getLength() );
                    buffer.reset();

                    return true;
                }
            }

        }

        return false;
    }

    @Override
    public LongWritable getCurrentKey() throws IOException, InterruptedException {

        return key;
    }

    @Override
    public Text getCurrentValue() throws IOException, InterruptedException {

        return value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {

        return ( float ) 0;
    }

    @Override
    public void close() throws IOException {

        inputStream.close();
    }

    private boolean findTag( byte[] tag, boolean child ) throws IOException {

        int i = 0;

        while ( i < tag.length ) {

            int data = inputStream.read();

            if ( data == -1 ) return false;

            if ( !child && i == 0 && inputStream.getPos() >= end ) return false;

            if ( data == tag[ i ] ) {

                i++;
                if ( child ) buffer.write( data );

            }

        }

        return true;
    }
}

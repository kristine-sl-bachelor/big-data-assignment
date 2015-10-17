package xml;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Scanner;

public class XmlRecordReader extends RecordReader< LongWritable, Text > {

    private String[] tags;

    private byte[][] tagsStart;
    private byte[][] tagsEnd;

    private long start;
    private long end;

    private DataOutputBuffer buffer;
    private FSDataInputStream input;

    private LongWritable key;
    private Text value;

    public XmlRecordReader( String... tags ) {

        this.tags = tags;
    }

    @Override
    public void initialize( InputSplit split, TaskAttemptContext context ) throws IOException, InterruptedException {

        tagsStart = new byte[ tags.length ][];
        tagsEnd = new byte[ tags.length ][];

        for ( int i = 0; i < tags.length; i++ ) {

            // Using StandardCharsets.UTF-8 negotiates the need for check for UnsupportedEncodingException

            tagsStart[ i ] = ( "<" + tags[ i ] ).getBytes( StandardCharsets.UTF_8 );
            tagsEnd[ i ] = ( "</" + tags[ i ] + ">" ).getBytes( StandardCharsets.UTF_8 );

        }

        FileSplit fileSplit = ( FileSplit ) split;

        start = fileSplit.getStart();
        end = start + fileSplit.getLength();

        Configuration config = context.getConfiguration();

        FileSystem fileSystem = fileSplit.getPath().getFileSystem( config );

        buffer = new DataOutputBuffer();
        input = fileSystem.open( fileSplit.getPath() );
        input.seek( start );
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {

        if ( input.getPos() >= end ) return false;

        if ( tagFound( tagsStart, false ) ) { // Find opening tag

            if ( tagFound( tagsEnd, true ) ) { // Find closing tag (data will be written to buffer because inTag = true)

                key = new LongWritable( input.getPos() );
                value = new Text();
                value.set( minimizeOutput( buffer.getData() ) );

                buffer.reset(); // Reset buffer for next stream

                return true;
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

        float total = ( float ) end - start;
        long read = input.getPos() - start;

        return read / total;
    }

    @Override
    public void close() throws IOException {

        input.close();
    }

    private boolean tagFound( byte[][] tags, boolean inTag ) throws IOException {

        int i = 0;

        boolean tagFound = false;

        while ( true ) {

            int data = input.read(); // Reads the next byte of data from the data stream

            if ( data == -1 ) return false;

            if ( inTag ) buffer.write( data ); // Data inside found tag

            tagFound = false;

            for ( byte[] tag : tags ) {

                if ( i < tag.length && tag[ i ] == data ) { // Byte in tag found

                    tagFound = true;
                    i++;

                    if ( i == tag.length ) { // Entire tag found

                        if ( !inTag ) buffer.write( tag ); // Already written to buffer if in tag
                        return true;
                    }

                    break;
                }
            }

            if ( !tagFound ) i = 0;

            if ( !inTag && input.getPos() >= end ) return false; // At the end of the file, no tag found

        }
    }

    private String minimizeOutput( byte[] formattedOutput ) {

        String formattedString = new String( formattedOutput, StandardCharsets.UTF_8 );

        Scanner scanner = new Scanner( formattedString );

        String minimizedOutput = "";

        while ( scanner.hasNextLine() ) minimizedOutput += scanner.nextLine().trim();

        return minimizedOutput;
    }
}

package _other.xml;

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

    private long start;
    private long end;

    private DataOutputBuffer buffer;
    private FSDataInputStream input;

    private LongWritable key;
    private Text value;

    private String startTag, endTag;

    /**
     * Formats the given tag, only needs to know the root tag of the dokument
     *
     * @param tag Name of the root tag, in this case: "dblp"
     */
    public XmlRecordReader( String tag ) {

        startTag = "<" + tag + ">";
        endTag = "</" + tag + ">";
    }

    /**
     * Initializes the necessary variables, and gets the {@link FSDataInputStream} past the root tag
     */
    @Override
    public void initialize( InputSplit split, TaskAttemptContext context ) throws IOException, InterruptedException {


        FileSplit fileSplit = ( FileSplit ) split;

        start = fileSplit.getStart();
        end = start + fileSplit.getLength();

        // Configure input
        Configuration config = context.getConfiguration();
        FileSystem fileSystem = fileSplit.getPath().getFileSystem( config );
        buffer = new DataOutputBuffer();
        input = fileSystem.open( fileSplit.getPath() );
        input.seek( start );

        // Gets input past root tag
        int i = 0;
        while ( i < startTag.length() ) {

            int in = input.read();

            if ( in == ( byte ) startTag.charAt( i ) ) i++;
            else i = 0;
        }
    }

    /**
     * @return The next child element of the root tag, as a String ("<article>...</article>")
     */
    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {

        // Make sure we skip any whitespaces placed before the current tag
        int temp = input.read();

        while ( temp != '<' ) {

            temp = input.read();
        }

        // Resets the buffer (buffer.reset() would not blank out values from last iteration)
        buffer = new DataOutputBuffer();

        buffer.write( temp );

        // Saves our current tag name, so that we can check closing tag later.
        // This negotiates the need to know all the tag names of the wanted tags, and makes the class highly reusable
        String currentTag = getCurrentTag();

        boolean inTag = false, closing = false;

        while ( input.getPos() < end ) {

            int in = input.read();

            // Bad input
            if ( in == -1 ) return false;

            if ( in == ( byte ) '<' ) inTag = true;
            else if ( in == ( byte ) '>' ) inTag = false;

            // Closing tag
            if ( inTag && in == ( byte ) '/' ) {

                closing = true;
                buffer.write( in );
            }

            if ( closing ) {

                String current = getCurrentTag();

                // Checks to see if the closing tag we have found matches the current tag name. If true, we have found the entire
                // child node, which is sent as a String with a Text object, to be parsed as needed in a mapper
                if ( current.equals( currentTag ) ) {

                    key = new LongWritable( input.getPos() );
                    value = new Text( minimizeOutput( buffer.getData() ) );

                    return true;
                }

                closing = false;

            } else {

                buffer.write( in );
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

    private String getCurrentTag() throws IOException {

        String output = "";

        while ( true ) {

            byte in = ( byte ) input.read();

            buffer.write( in );

            // Tag will only be one word, so if we find a space or closing brace, we know we have found the entire tag, and
            // can return it.
            if ( in == ( byte ) ' ' || in == ( byte ) '>' ) return output;

            output += ( char ) in;
        }
    }

    /**
     * @return A minimized version of the xml, without any unnecessary whitespaces
     */
    private String minimizeOutput( byte[] formattedOutput ) {

        String formattedString = new String( formattedOutput, StandardCharsets.UTF_8 );

        Scanner scanner = new Scanner( formattedString );

        String minimizedOutput = "";

        while ( scanner.hasNextLine() ) minimizedOutput += scanner.nextLine().trim();

        return minimizedOutput;
    }
}

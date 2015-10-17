package xml;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Scanner;

import static org.junit.Assert.assertEquals;

public class XmlInputFormatTest {

    Configuration config;
    RecordReader reader;

    private final File inputFile = new File( "src/test/resources/test_data_full.xml" );
    private final File minimizedInputFile = new File( "src/test/resources/test_data_full_minimized.xml" );
    private final File minimizedOutputFile = new File( "src/test/resources/test_data_article_minimized.xml" );
    private String outputString;

    @Before
    public void setup() throws FileNotFoundException {

        outputString = "";

        Scanner scanner = new Scanner( minimizedOutputFile );
        outputString += scanner.nextLine().trim();

        config = new Configuration( false );
        config.set( "fs.default.name", "file:///" );
    }

    @Test
    public void testFormattedFiles() throws IOException, InterruptedException {

        setupReader( inputFile );

        reader.nextKeyValue();

        assertEquals( outputString, reader.getCurrentValue().toString() );

    }

    private void setupReader( File inputFile ) throws IOException, InterruptedException {

        Path path = new Path( inputFile.getAbsoluteFile().toURI() );
        FileSplit split = new FileSplit( path, 0, inputFile.length(), null );

        InputFormat input = ReflectionUtils.newInstance( XmlInputFormat.class, config );
        TaskAttemptContext context = new TaskAttemptContextImpl( config, new TaskAttemptID() );
        reader = input.createRecordReader( split, context );

        reader.initialize( split, context );
    }
}

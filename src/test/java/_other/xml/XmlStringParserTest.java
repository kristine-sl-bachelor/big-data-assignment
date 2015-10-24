package _other.xml;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.Test;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.File;
import java.io.IOException;

import static org.junit.Assert.assertTrue;

public class XmlStringParserTest {

    RecordReader reader;

    private final File inputFile = new File( "src/test/resources/test_files/test_data_full.xml" );

    @Test
    public void testParser() throws IOException, InterruptedException, ParserConfigurationException, SAXException {

        setupReader( inputFile );

        while ( reader.nextKeyValue() ) {

            XmlStringParser parser = new XmlStringParser( reader.getCurrentValue().toString() );

            assertTrue( parser.getChildElements().getLength() > 0 );
        }
    }

    private void setupReader( File inputFile ) throws IOException, InterruptedException {

        Configuration config = new Configuration();

        Path path = new Path( inputFile.getAbsoluteFile().toURI() );
        FileSplit split = new FileSplit( path, 0, inputFile.length(), null );

        InputFormat input = ReflectionUtils.newInstance( XmlInputFormat.class, config );
        TaskAttemptContext context = new TaskAttemptContextImpl( config, new TaskAttemptID() );
        reader = input.createRecordReader( split, context );

        reader.initialize( split, context );
    }
}

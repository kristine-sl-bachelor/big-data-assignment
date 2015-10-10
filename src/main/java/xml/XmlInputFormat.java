package xml;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import record_readers.XmlRecordReader;

import java.io.IOException;

public class XmlInputFormat extends TextInputFormat {

    @Override
    public RecordReader< LongWritable, Text > getRecordReader( InputSplit genericSplit, JobConf job, Reporter reporter ) throws IOException {

        return new XmlRecordReader();
    }
}

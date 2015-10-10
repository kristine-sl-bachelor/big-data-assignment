package warmup.word_character_count;

import models.Word;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;


public class WordCharacterCountMapper extends Mapper< LongWritable, Text, Text, IntWritable > {

    private final Text WORD = new Text();

    /**
     *
     */
    @Override
    protected void map( LongWritable key, Text value, Context context ) throws IOException, InterruptedException {

        String text = value.toString();
        StringTokenizer tokenizer = new StringTokenizer( text );

        while ( tokenizer.hasMoreTokens() ) {

            WORD.set( tokenizer.nextToken() );

            if ( Word.isWord( WORD.toString() ) ) {

                WORD.set( new Text( Word.cleanWord( WORD.toString() ) ) );

                context.write( WORD, new IntWritable( WORD.getLength() ) );

            }
        }
    }
}

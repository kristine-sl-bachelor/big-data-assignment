package wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;


public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable ONE = new IntWritable(1);
    private final Text WORD = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String text = value.toString();
        StringTokenizer tokenizer = new StringTokenizer( text );

        while(tokenizer.hasMoreTokens()) {

            WORD.set( tokenizer.nextToken() );

            if( isWord( WORD.toString() ) ) {

                WORD.set( new Text( cleanWord( WORD.toString() ) ) );
                context.write( WORD, ONE );
            }
        }
    }


    /**
     * Checks if a given string is a word, by checking that the first character of the string
     * is a letter.
     *
     * @param key   The word to be checked
     * @return      Whether or not the string is a word
     */
    private boolean isWord( String key ) {

        return Character.isLetter( key.charAt( 0 ) );
    }

    /**
     * Iterates through all the characters in a word, and removes any that are not letters.
     *
     * @param word
     * @return      A version of word without any characters that are not letters
     */
    private String cleanWord( String word ) {

        String cleanWord = "";

        for( char c : word.toCharArray() ) {

            cleanWord = Character.isLetter( c ) ? cleanWord + c : cleanWord;
        }

        return cleanWord;
    }
}

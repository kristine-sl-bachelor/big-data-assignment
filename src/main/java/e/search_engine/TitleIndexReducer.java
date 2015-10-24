package e.search_engine;

import _other.helpers.Word;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class TitleIndexReducer extends Reducer< Text, Text, Text, Text > {

    private final File file = new File( "src/main/resources/stopwords.txt" );
    private List< String > excludedWords;

    @Override
    protected void setup( Context context ) throws IOException, InterruptedException {

        Scanner scanner = new Scanner( file );
        excludedWords = new ArrayList< String >();

        while ( scanner.hasNextLine() ) {

            excludedWords.add( Word.cleanWord( scanner.nextLine().toLowerCase() ) );
        }
    }

    @Override
    protected void reduce( Text key, Iterable< Text > values, Context context ) throws IOException, InterruptedException {

        String word = key.toString().toLowerCase();

        if ( !excludedWords.contains( Word.cleanWord( word ) ) ) {

            String output = "";

            for ( Text value : values ) {

                if ( output.length() > 0 ) output += ";";

                output += value.toString();
            }

            context.write( new Text( word ), new Text( output ) );
        }
    }
}

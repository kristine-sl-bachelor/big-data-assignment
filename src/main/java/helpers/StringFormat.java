package helpers;

public class StringFormat {

    public static final String FORMAT_WORD = "%-50s";

    public static String format( String string, String format ) {

        return String.format( format, string );
    }
}

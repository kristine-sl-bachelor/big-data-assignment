package models;

public class Word {

    /**
     * Checks if a given string is a word, by checking that the first character of the string
     * is a letter.
     *
     * @param key   The word to be checked
     * @return      Whether or not the string is a word
     */
    public static boolean isWord( String key ) {

        return Character.isLetter( key.charAt( 0 ) );
    }

    /**
     * Iterates through all the characters in a word, and removes any that are not letters.
     *
     * @param word
     * @return      A version of word without any characters that are not letters
     */
    public static String cleanWord( String word ) {

        String cleanWord = "";

        for( char c : word.toCharArray() ) {

            cleanWord = Character.isLetter( c ) ? cleanWord + c : cleanWord;
        }

        return cleanWord;
    }
}

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author Konstantinos Chasiotis
 */
public class TextProcessing {

    private static Stemmer stemText = new Stemmer();
    private static StopwordRemover removeStopwords = new StopwordRemover();

    public static String SanitizeText(String Text) {
        String temp = Text.trim().replaceAll("\\p{Punct}+", " ");

        temp = temp.toLowerCase();
        return temp;
    }

    public static String Stemmer(String inputText) {
        return stemText.Run(inputText);
    }

    public static String RemoveStopwords(String inputText) {
        return removeStopwords.Run(inputText);
    }
}

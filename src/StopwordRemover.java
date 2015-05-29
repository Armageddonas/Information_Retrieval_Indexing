/**
 * Homework 3 10/03/14 CSC320
 *
 * @author Adam Bavosa
 */
import java.io.*;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.swing.*;

/**
 * Stopword remover. Removes stopwords from tokenizer input (token.txt)
 */
public class StopwordRemover {

    public static String Run(String textLine) {
        BufferedReader stopFile;
        try {
            stopFile = new BufferedReader(new FileReader("stopwords.txt"));
            ArrayList<String> stopWords = new ArrayList();

            boolean isStopword = false;
            String sanitizedText = "";

            //<editor-fold defaultstate="collapsed" desc="Load stopwords">
            String stopWord;
            while ((stopWord = stopFile.readLine()) != null) {
                stopWords.add(stopWord);
            }
            stopFile.close();
            //</editor-fold>

            String[] tokenLineWords = textLine.split(" ");

            for (int j = 0; j < tokenLineWords.length; j++) {
                //<editor-fold defaultstate="collapsed" desc="Chech if word is stopword">
                isStopword = false;
                for (int i = 0; i < stopWords.size(); i++) {
                    if (stopWords.get(i).equals(tokenLineWords[j])) {
                        isStopword = true;
                    }
                }
                //</editor-fold>
                //<editor-fold defaultstate="collapsed" desc="Keep word if not a stopword">
                if (isStopword == false) {
                    sanitizedText += tokenLineWords[j] + " ";
                }
                //</editor-fold>
            }

            return sanitizedText;
        } catch (FileNotFoundException ex) {
            Logger.getLogger(StopwordRemover.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            Logger.getLogger(StopwordRemover.class.getName()).log(Level.SEVERE, null, ex);
        }
        return null;
    }
}

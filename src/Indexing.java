
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
/**
 *
 * @author Konstantinos Chasiotis
 */
public class Indexing {

    String dbName;
    final String dbPath = "databases/";
    final String collectionPath = "collection/";

    public Indexing(int database) {
        dbName = "index" + database;
    }

    public void Run() {
        InitDatabase();

        String filenames[] = GetFileNames();

        //for (int i = 0; i < filenames.length; i++) {
        for (int i = 2242; i < 2243; i++) {//debug
            String doc = LoadDocument(collectionPath + filenames[i]);
            ProccessDocument(doc);
            InsertDocumentToDB();
        }
    }

    private void InitDatabase() {

        //<editor-fold defaultstate="collapsed" desc="if the directory does not exist, create it">
        File dir = new File(dbPath);
        if (!dir.exists()) {
            System.out.println("creating directory: " + dbPath);
            boolean result = false;

            try {
                dir.mkdir();
                result = true;
            } catch (SecurityException se) {
                //handle it
            }
            if (result) {
                System.out.println("DIR created");
            }
        }
        //</editor-fold>

        Connection c = null;
        try {
            Class.forName("org.sqlite.JDBC");
            c = DriverManager.getConnection("jdbc:sqlite:" + dbPath + dbName + ".db");
        } catch (Exception e) {
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
            System.exit(0);
        }
        System.out.println("Opened database successfully");
    }

    private String LoadDocument(String filepath) {

        ArrayList<Integer> relDocs = new ArrayList();
        String document = "";
        try {

            BufferedReader in = new BufferedReader(new FileReader(filepath));
            String line;
            while ((line = in.readLine()) != null) {
                if (line.length() > 0) {
                    document += line + "\n";

                }
            }
            in.close();
        } catch (IOException ex) {
            Logger.getLogger(Indexing.class.getName()).log(Level.SEVERE, null, ex);
            return null;
        }
        //System.out.println(document);
        return document;
    }

    private Collection_Document ProccessDocument(String doc) {
        Collection_Document processed_doc = new Collection_Document();
        String insidePreText = null;

        //<editor-fold defaultstate="collapsed" desc="Extract text inside <pre>">
        Matcher extractPre = Pattern.compile("<pre>\n(.*(.|\\s)*)<\\/pre>").matcher(doc);
        if (extractPre.find()) {
            System.out.println("The content is:\n" + extractPre.group(1) + "\n------------------------------------------------------");
            insidePreText = extractPre.group(1);
        }
            //</editor-fold>
        //System.exit(-1);

        //<editor-fold defaultstate="collapsed" desc="Extract text line by line">
        String[] lines = insidePreText.split("\n");
        /*for (int i = 0; i < lines.length; i++) {
         System.out.println("line"+i+" "+lines[i]+"\n");
         }*/
        processed_doc.Title = lines[0];
        for (int i = 1; i < lines.length; i++) {

            String bookReferences = lines[i];
            
            //<editor-fold defaultstate="collapsed" desc="Save words">
            Matcher RgxGetWords = Pattern.compile("[a-zA-Z|0-9]+").matcher(bookReferences);
            while (RgxGetWords.find()) {
                //System.out.println("Word: " + RgxGetWords.group());
                processed_doc.Words.add(RgxGetWords.group());
            }
            //</editor-fold>
            
            //If a book reference is encountered break
            Matcher RgxBookReferences = Pattern.compile("CA[0-9]{6}").matcher(bookReferences);
            if (RgxBookReferences.find()) {
                break;
            } 

        }
        //</editor-fold>
        System.out.println(processed_doc.toString());
        return processed_doc;
    }

    private void InsertDocumentToDB() {

    }

    private String[] GetFileNames() {
        File folder = new File(collectionPath);
        File[] listOfFiles = folder.listFiles();
        String[] filenames = new String[listOfFiles.length];

        for (int i = 0; i < listOfFiles.length; i++) {
            if (listOfFiles[i].isFile()) {
                //System.out.println("File " + listOfFiles[i].getName());
                filenames[i] = listOfFiles[i].getName();
            } else if (listOfFiles[i].isDirectory()) {
                //System.out.println("Directory " + listOfFiles[i].getName());
            }
        }
        return filenames;
    }

    public WordInfo indexOf(String Word) {
        return null;
    }
}


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
        System.out.println(document);
        return document;
    }

    private Collection_Document ProccessDocument(String doc) {
        Collection_Document processed_doc = new Collection_Document();

        //<editor-fold defaultstate="collapsed" desc="Get text only (inside pre)">
        String pattern = "<pre>\n(.*)";

        Pattern r = Pattern.compile(pattern);

        // Now create matcher object.
        Matcher m = r.matcher(doc);
        if (m.find()) {
            System.out.println("The title is: " + m.group(0));
        } else {
            System.out.println("NO MATCH");
        }
        //</editor-fold>
        
        
        
        // String to be scanned to find the pattern.
        /*String line = "This order was placed for QT3000! OK?";
         String pattern = "(.*)(\\d+)(.*)";

         // Create a Pattern object
         Pattern r = Pattern.compile(pattern);

         // Now create matcher object.
         Matcher m = r.matcher(line);*/
        return null;
    }

    private void InsertDocumentToDB() {

    }

    public Indexing(int database) {
        dbName = "index" + database;
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

    public void Run() {
        InitDatabase();

        String filenames[] = GetFileNames();

        //for (int i = 0; i < filenames.length; i++) {
        for (int i = 0; i < 1; i++) {//debug
            String doc = LoadDocument(collectionPath + filenames[i]);
            ProccessDocument(doc);
            InsertDocumentToDB();
        }
    }

    public String[] loadUrls() {

        return null;
    }

    public WordInfo indexOf(String Word) {
        return null;
    }
}

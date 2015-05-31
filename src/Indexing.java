
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.swing.JProgressBar;


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
    int database;
    JProgressBar prBar = null;
    final String dbPath = "databases/";
    final String collectionPath = "collection/";
    Connection conn = null;
    PreparedStatement stmWordInCurDoc;
    PreparedStatement prep;
    PreparedStatement stmWordInDB;
    PreparedStatement stmWord;
    PreparedStatement stmWordInDoc;

    public Indexing(int database) {
        dbName = "index" + database;
        this.database = database;
    }

    public Indexing(int database, JProgressBar prBar) {
        dbName = "index" + database;
        this.database = database;
        this.prBar = prBar;
    }

    public void Run() {
        InitDatabase();

        String filenames[] = GetFileNames();

        prBar.setMaximum(filenames.length + filenames.length / 6);
        for (int i = 0; i < filenames.length; i++) {
            //for (int i = 0; i < 400; i++) {//debug
            if (prBar != null) {
                prBar.setValue(i);
            }
            System.out.println("Database: " + database + " Document " + (i + 1) + "\tof " + filenames.length);
            String doc = LoadDocument(collectionPath + filenames[i]);
            Collection_Document processedDoc = ProccessDocument(doc);
            InsertDocumentToDB(processedDoc);
            System.gc();
        }

        //System.exit(-5);//!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
        try {
            //<editor-fold defaultstate="collapsed" desc="Get doc insert id">
            //<editor-fold defaultstate="collapsed" desc="Update ctf,df">
            PreparedStatement stmUpdateCTF = conn.prepareStatement("update Word set "
                    + "ctf=(select sum(TF) from WordInDoc where WordInDoc.idWord=Word.idWord),"
                    + "df=(select count(idWord) from WordInDoc where WordInDoc.idWord=Word.idWord);");
            //stmUpdateCTF.setString(1, processedDoc.Title);

            stmUpdateCTF.executeUpdate();
            //</editor-fold>

            prBar.setMaximum(filenames.length);
        } catch (SQLException ex) {
            Logger.getLogger(Indexing.class.getName()).log(Level.SEVERE, null, ex);
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

        //<editor-fold defaultstate="collapsed" desc="Drop database">
        File dbFile = new File(dbPath + dbName + ".db");
        if (dbFile.exists()) {
            dbFile.delete();
        }
        //</editor-fold>

        try {
            Class.forName("org.sqlite.JDBC");
            conn = DriverManager.getConnection("jdbc:sqlite:" + dbPath + dbName + ".db");
            //Gain speed
            //conn.prepareStatement("PRAGMA synchronous=OFF;").execute();
            conn.prepareStatement("PRAGMA journal_mode = MEMORY;").execute();
            //conn.commit();
            //<editor-fold defaultstate="collapsed" desc="Create tables">
            Statement stat = conn.createStatement();
            stat.execute("  CREATE TABLE Word (\n"
                    + "  idWord INTEGER PRIMARY KEY AUTOINCREMENT,\n"
                    + "  Name VARCHAR(22) NOT NULL,\n"
                    + "  ctf INT NULL,\n"
                    + "  df INT NULL\n"
                    + "  );");
            stat.execute("CREATE TABLE Document (\n"
                    + "  idDocument INTEGER PRIMARY KEY AUTOINCREMENT,\n"
                    + "  DocLength INT NULL,\n"
                    + "  Title VARCHAR(130) NULL\n"
                    + "  );");
            stat.execute("  \n"
                    + "  CREATE TABLE WordInDoc (\n"
                    + "  idWord INT NOT NULL,\n"
                    + "  idDocument INT NOT NULL,\n"
                    + "  TF INT NOT NULL,\n"
                    + "  PRIMARY KEY (idWord, idDocument),\n"
                    + "  CONSTRAINT FK_Document\n"
                    + "    FOREIGN KEY (idDocument)\n"
                    + "    REFERENCES Document (idDocument)\n"
                    + "    ON DELETE NO ACTION\n"
                    + "    ON UPDATE NO ACTION,\n"
                    + "  CONSTRAINT FK_Word\n"
                    + "    FOREIGN KEY (idWord)\n"
                    + "    REFERENCES Word (idWord)\n"
                    + "    ON DELETE NO ACTION\n"
                    + "    ON UPDATE NO ACTION);");
            //</editor-fold>
        } catch (Exception e) {
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
            System.exit(0);
        }
        System.out.println("Opened database successfully");

        try {
            //<editor-fold defaultstate="collapsed" desc="Init prepared statements">
            stmWordInCurDoc = conn.prepareStatement(""
                    + "select Word.idWord from word, Document, WordInDoc WordInDoc "
                    + "where WordInDoc.idDocument=Document.idDocument and WordInDoc.idWord=Word.idWord and Name=? and Document.idDocument=?");

            prep = conn.prepareStatement("insert into Document(Title,DocLength) values (?,?);");
            stmWordInDB = conn.prepareStatement("select idWord from word WordInDoc where Name=?");
            stmWord = conn.prepareStatement(
                    "insert into Word(Name) values (?);");
            stmWordInDoc = conn.prepareStatement(
                    "insert into WordInDoc(idWord,idDocument,TF) values (?,?,?);");

//</editor-fold>
        } catch (SQLException ex) {
            Logger.getLogger(Indexing.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    private String LoadDocument(String filepath) {

        String document = "";
        try {

            BufferedReader in = new BufferedReader(new FileReader(filepath));
            String line;
            boolean flagPre = false;
            while ((line = in.readLine()) != null) {
                if (line.length() > 0) {
                    if (flagPre == false && line.equals("<pre>")) {
                        flagPre = true;
                        continue;
                    } else if (flagPre == true && line.equals("</pre>")) {
                        break;
                    }
                    if (flagPre == true) {
                        document += line + "\n";
                    }
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
        String insidePreText = doc;
        
        //<editor-fold defaultstate="collapsed" desc="Extract text line by line">
        String[] lines = insidePreText.split("\n");
        processed_doc.Title = TextProcessing.SanitizeText(lines[0]);
        {
            for (int i = 0; i < lines.length; i++) {

                //<editor-fold defaultstate="collapsed" desc="Match text to Database">
                String bookReferences = lines[i];
                lines[i] = TextProcessing.SanitizeText(lines[i]);

                switch (database) {
                    case 0: {
                        break;
                    }
                    case 1: {
                        lines[i] = TextProcessing.Stemmer(lines[i]);
                        break;
                    }
                    case 2: {
                        lines[i] = TextProcessing.RemoveStopwords(lines[i]);
                        break;
                    }
                    case 3: {
                        lines[i] = TextProcessing.RemoveStopwords(lines[i]);
                        lines[i] = TextProcessing.Stemmer(lines[i]);
                        break;
                    }
                    default: {
                        System.out.println("Wrong database number");
                        System.exit(-3);
                        break;
                    }
                }
            //</editor-fold>

                //<editor-fold defaultstate="collapsed" desc="Save words">
                Matcher RgxGetWords = Pattern.compile("[A-Za-z|0-9]+").matcher(lines[i]);
                while (RgxGetWords.find()) {
                    processed_doc.Words.add(RgxGetWords.group());
                }
            //</editor-fold>

                //If a book reference is encountered break
                Matcher RgxBookReferences = Pattern.compile("ca[0-9]{6}").matcher(bookReferences.toLowerCase());
                if (RgxBookReferences.find()) {
                    break;
                }

            }
        }
        //</editor-fold>
        //System.out.println("The content is:\n" + extractPre.group(1) + "\n------------------------------------------------------");
        //System.out.println(processed_doc.toString());
        return processed_doc;
    }

    private void InsertDocumentToDB(Collection_Document processedDoc) {

        try {
            int docID;
            int wordID;

            //<editor-fold defaultstate="collapsed" desc="Insert document">
            prep.setString(1, processedDoc.Title);
            prep.setInt(2, processedDoc.Words.size());
            prep.executeUpdate();
            //</editor-fold>

            //<editor-fold defaultstate="collapsed" desc="Get doc insert id">
            ResultSet res = conn.createStatement().executeQuery("select last_insert_rowid() as id");
            res.next();
            docID = res.getInt("id");
            res.close();
            //</editor-fold>

            //<editor-fold defaultstate="collapsed" desc="Insert values in Word and WordInDoc">
            for (int i = 0; i < processedDoc.Words.size(); i++) {
                //<editor-fold defaultstate="collapsed" desc="Check if word is in database">

                stmWordInDB.setString(1, processedDoc.Words.get(i));
                ResultSet rtsWordInDB = stmWordInDB.executeQuery();

                //</editor-fold>                
                if (rtsWordInDB.next() == false) {

                    //<editor-fold defaultstate="collapsed" desc="Insert word">
                    stmWord.setString(1, processedDoc.Words.get(i));
                    stmWord.executeUpdate();
                    //</editor-fold>

                    //<editor-fold defaultstate="collapsed" desc="Get word insert id">
                    ResultSet rtsWordID = conn.createStatement().executeQuery("select last_insert_rowid() as id");
                    rtsWordID.next();
                    wordID = rtsWordID.getInt("id");
                    rtsWordID.close();
                    //</editor-fold>
                } else {
                    if (WordInDoc(processedDoc.Words.get(i), docID) == true) {
                        continue;
                    }
                    //System.out.println("Dublicate: " + processedDoc.Words.get(i));
                    wordID = rtsWordInDB.getInt("idWord");
                }

                //<editor-fold defaultstate="collapsed" desc="WordInDoc">
                stmWordInDoc.setInt(1, wordID);
                stmWordInDoc.setInt(2, docID);
                stmWordInDoc.setInt(3, processedDoc.CalcTF(processedDoc.Words.get(i)));
                stmWordInDoc.executeUpdate();
                //</editor-fold>
            }
            //</editor-fold>
        } catch (SQLException ex) {
            Logger.getLogger(Indexing.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    private boolean WordInDoc(String word, int docID) {
        try {
            //<editor-fold defaultstate="collapsed" desc="Check if word is in current document">

            stmWordInCurDoc.setString(1, word);
            stmWordInCurDoc.setInt(2, docID);
            ResultSet rtsWordInCurDoc = stmWordInCurDoc.executeQuery();

            if (rtsWordInCurDoc.next()) {
                return true;
            }
            //</editor-fold> 
        } catch (SQLException ex) {
            Logger.getLogger(Indexing.class.getName()).log(Level.SEVERE, null, ex);
        }
        return false;
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


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
    int database;
    final String dbPath = "databases/";
    final String collectionPath = "collection/";
    Connection conn = null;

    public Indexing(int database) {
        dbName = "index" + database;
        this.database = database;
    }

    public void Run() {
        InitDatabase();

        String filenames[] = GetFileNames();

        for (int i = 0; i < filenames.length; i++) {
            //for (int i = 2241; i < 2243; i++) {//debug
            String doc = LoadDocument(collectionPath + filenames[i]);
            Collection_Document processedDoc = ProccessDocument(doc);
            InsertDocumentToDB(processedDoc);
        }

        System.exit(-5);//!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
        try {
            //<editor-fold defaultstate="collapsed" desc="Get doc insert id">
            ResultSet res = conn.createStatement().executeQuery("select Name from Word");
            while (res.next()) {
                String temp = res.getString("Name");

                //<editor-fold defaultstate="collapsed" desc="Insert ctf,df">
                PreparedStatement stmUpdateCTF = conn.prepareStatement("update Word set "
                        + " ctf=(select sum(TF) from WordInDoc where WordInDoc.idWord=Word.idWord),"
                        + "df=(select count(idWord) from WordInDoc where WordInDoc.idWord=Word.idWord);");
                //stmUpdateCTF.setString(1, processedDoc.Title);

                stmUpdateCTF.executeUpdate();
                //</editor-fold>
            }
            res.close();
            //</editor-fold>

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

    private String LoadQuery() {

        ArrayList<Integer> relDocs = new ArrayList();
        String query = "";
        try {

            BufferedReader in = new BufferedReader(new FileReader(dbPath + "QueryDb.sql"));
            String line;
            while ((line = in.readLine()) != null) {
                query += line + "\n";

            }
            in.close();
        } catch (IOException ex) {
            Logger.getLogger(Indexing.class.getName()).log(Level.SEVERE, null, ex);
            return null;
        }
        //System.out.println(document);
        return query;
    }

    private Collection_Document ProccessDocument(String doc) {
        Collection_Document processed_doc = new Collection_Document();
        String insidePreText = null;

        //<editor-fold defaultstate="collapsed" desc="Extract text inside <pre>">
        Matcher extractPre = Pattern.compile("<pre>\n(.*(.|\\s)*)<\\/pre>").matcher(doc);
        if (extractPre.find()) {
            insidePreText = extractPre.group(1);
        }
            //</editor-fold>
        //System.exit(-1);

        //<editor-fold defaultstate="collapsed" desc="Extract text line by line">
        String[] lines = insidePreText.split("\n");

        processed_doc.Title = TextProcessing.SanitizeText(lines[0]);
        for (int i = 0; i < lines.length; i++) {

            //<editor-fold defaultstate="collapsed" desc="Match text to Database">
            String bookReferences = TextProcessing.SanitizeText(lines[i]);

            switch (database) {
                case 1: {
                    lines[i] = TextProcessing.Stemmer(lines[i]);
                }
                case 2: {
                    lines[i] = TextProcessing.RemoveStopwords(lines[i]);
                }
                case 3: {
                    lines[i] = TextProcessing.RemoveStopwords(lines[i]);
                    lines[i] = TextProcessing.Stemmer(lines[i]);
                }
            }
            //</editor-fold>

            //<editor-fold defaultstate="collapsed" desc="Save words">
            Matcher RgxGetWords = Pattern.compile("[a-zA-Z|0-9]+").matcher(bookReferences);
            while (RgxGetWords.find()) {
                processed_doc.Words.add(RgxGetWords.group());
            }
            //</editor-fold>

            //If a book reference is encountered break
            Matcher RgxBookReferences = Pattern.compile("[C|c][A|a][0-9]{6}").matcher(bookReferences);
            if (RgxBookReferences.find()) {
                break;
            }

        }
        //</editor-fold>
        //System.out.println("The content is:\n" + extractPre.group(1) + "\n------------------------------------------------------");
        System.out.println(processed_doc.toString());
        return processed_doc;
    }

    private void InsertDocumentToDB(Collection_Document processedDoc) {

        try {
            int docID;
            int wordID;

            //<editor-fold defaultstate="collapsed" desc="Insert document">
            PreparedStatement prep = conn.prepareStatement("insert into Document(Title,DocLength) values (?,?);");

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
                PreparedStatement stmWordInDB = conn.prepareStatement("select idWord from word WordInDoc where Name=?");

                stmWordInDB.setString(1, processedDoc.Words.get(i));
                ResultSet rtsWordInDB = stmWordInDB.executeQuery();

                //</editor-fold>                
                if (rtsWordInDB.next() == false) {

                    //<editor-fold defaultstate="collapsed" desc="Insert word">
                    PreparedStatement stmWord = conn.prepareStatement(
                            "insert into Word(Name) values (?);");

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
                    //<editor-fold defaultstate="collapsed" desc="Check if word is in current document">
                    PreparedStatement stmWordInCurDoc = conn.prepareStatement(""
                            + "select Word.idWord from word, Document, WordInDoc WordInDoc "
                            + "where WordInDoc.idDocument=Document.idDocument and WordInDoc.idWord and Word.idWord and Name=? and Document.idDocument=?");

                    stmWordInCurDoc.setString(1, processedDoc.Words.get(i));
                    stmWordInCurDoc.setInt(2, docID);
                    ResultSet rtsWordInCurDoc = stmWordInCurDoc.executeQuery();
                    if (rtsWordInCurDoc.next() == true) {
                        continue;
                    }
                    //</editor-fold>  

                    //System.out.println("Dublicate: " + processedDoc.Words.get(i));
                    wordID = rtsWordInDB.getInt("idWord");
                }

                //<editor-fold defaultstate="collapsed" desc="WordInDoc">
                PreparedStatement stmWordInDoc = conn.prepareStatement(
                        "insert into WordInDoc(idWord,idDocument,TF) values (?,?,?);");

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

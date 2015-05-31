
import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.logging.Level;
import java.util.logging.Logger;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
/**
 *
 * @author Konstantinos Chasiotis
 */
public class DBSearch {

    String dbName;
    int database;
    final String dbPath = "databases/";
    Connection conn = null;

    public void InitDatabase(int database) {
        dbName = "index" + database;
        this.database = database;
    }

    public boolean InitDbCon() {

        //<editor-fold defaultstate="collapsed" desc="Check if db exists">
        File db = new File(dbPath + dbName + ".db");
        if (!db.exists()) {
            return false;
        }
        //</editor-fold>

        try {
            Class.forName("org.sqlite.JDBC");
            conn = DriverManager.getConnection("jdbc:sqlite:" + dbPath + dbName + ".db");
            //Gain speed
        } catch (Exception e) {
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
            System.exit(0);
        }
        System.out.println("Opened database successfully");
        return true;
    }

    public String DbInfoOf(String Keyword) {
        String results = "";
        //<editor-fold defaultstate="collapsed" desc="Process text">
        Keyword = TextProcessing.SanitizeText(Keyword);

        switch (database) {
            case 0: {
                break;
            }
            case 1: {
                Keyword = TextProcessing.Stemmer(Keyword);
                break;
            }
            case 2: {
                Keyword = TextProcessing.RemoveStopwords(Keyword);
                break;
            }
            case 3: {
                Keyword = TextProcessing.RemoveStopwords(Keyword);
                Keyword = TextProcessing.Stemmer(Keyword);
                break;
            }
            default: {
                System.out.println("Wrong database number");
                System.exit(-3);
                break;
            }
        }
        //</editor-fold>

        System.out.println(Keyword);
        try {
            //<editor-fold defaultstate="collapsed" desc="Find ctf and df">
            PreparedStatement stmDfCTF = conn.prepareStatement("select ctf, df from word where Name=?");

            stmDfCTF.setString(1, Keyword);
            ResultSet rtsDfCTF = stmDfCTF.executeQuery();
            if (rtsDfCTF.next()) {
                results += "CTF: " + rtsDfCTF.getInt("ctf") + "\t";
                results += "DF: " + rtsDfCTF.getInt("df") + "\n";
            } else {
                return "Keyword not found";
            }
            //</editor-fold>    

            //<editor-fold defaultstate="collapsed" desc="Find Tilte,DocLenght and tf">
            PreparedStatement stmDocs = conn.prepareStatement("select Title, DocLength, TF from Word, WordInDoc, Document "
                    + "where WordInDoc.idDocument=Document.idDocument and WordInDoc.idWord=Word.idWord and Name=?");

            stmDocs.setString(1, Keyword);
            ResultSet rtsDocs = stmDocs.executeQuery();
            while (rtsDocs.next()) {
                results += "Tilte " + "\t";
                results += rtsDocs.getString("Title") + "\t";
                results += "DocLenght " + "\t";
                results += rtsDocs.getString("DocLength") + "\t";
                results += "TF " + "\t";
                results += rtsDocs.getInt("TF") + "\n";
            }
            //</editor-fold>                
        } catch (SQLException ex) {
            Logger.getLogger(DBSearch.class.getName()).log(Level.SEVERE, null, ex);
            System.exit(5);
        }
        return results;
    }
}

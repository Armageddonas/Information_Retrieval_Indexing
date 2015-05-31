
import java.util.ArrayList;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
/**
 *
 * @author Konstantinos Chasiotis
 */
public class Collection_Document {

    String Title;
    int TF;
    int DocSize;
    //int CTF;
    ArrayList<String> Words = new ArrayList();

    @Override
    public String toString() {
        return "Collection_Document{" + "Title=" + Title + ", Words=" + Words + '}';
    }

    public int CalcTF(String Word) {
        return java.util.Collections.frequency(Words, Word);
    }
}

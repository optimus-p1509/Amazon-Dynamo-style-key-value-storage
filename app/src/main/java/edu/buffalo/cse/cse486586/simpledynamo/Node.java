package edu.buffalo.cse.cse486586.simpledynamo;

import android.util.Log;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Formatter;

public class Node implements Comparable<Node>{
    String myPort;
    String successor1Port;
    String successor2Port;
    ArrayList portList = new ArrayList();

    public Node(){

    }

    public Node(String myPort){
        this.myPort=myPort;
        this.successor1Port = myPort;
        this.successor2Port = myPort;
    }

    public Node(String myPort, String[] nodeList){
        this.myPort = myPort;
        ArrayList<String> pl = new ArrayList<String>(Arrays.asList(nodeList));
        this.portList = pl;
        int len = pl.size();
        int succ1Index = (pl.indexOf(myPort)+1)%len;
        this.successor1Port = pl.get(succ1Index);
        int succ2Index = (pl.indexOf(myPort)+2)%len;
        this.successor2Port = pl.get(succ2Index);
    }

    @Override
    public String toString() {
        return myPort + "---" +
                successor1Port + "---" +
                successor2Port;
    }

    @Override
    public int compareTo(Node another) {
        try {
            return (genHash(this.myPort).compareTo(genHash(another.myPort)));
        } catch (NoSuchAlgorithmException e) {
            Log.i("NSA", e.toString());
        }
        return 0;
    }

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }
}
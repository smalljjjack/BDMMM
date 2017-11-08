import java.io.*;
import java.util.Random.*;
import java.util.*;


public class TransactionsGenerator{
    public static void main(String[] args) throws Exception{
        File output = new File("Transactions");
        FileOutputStream fs = new FileOutputStream(output);
        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fs));
        bw.write("TransID,custID,TransTotal,TransNumItems,TranDesc");
        bw.newLine();
        for(int i = 1; i <= 5000000; i++){
            Transaction t = new Transaction(i);            
            bw.write(t.toString());
            bw.newLine();
        }
        bw.flush();
        bw.close();
    }

    public static class Transaction{
        private final String chars_List = 
        "zxcvbnmasdfghjklqwertyuiopZXCVBNMASDFGHJKLQWERTYUIOP1234567890"; 
        private Random r = new Random();

        public int TransID;
        public int CustID;
        public float TransTotal;
        public int TransNumItems;
        public String TransDesc;

        public Transaction(int ID){
            this.TransID = ID;
            this.CustID = r.nextInt(50000);
            this.TransTotal = r.nextFloat()*990+10;
            this.TransNumItems = r.nextInt(10)+1;
            
            StringBuilder sb = new StringBuilder();
            int length = r.nextInt(30)+20;

            for(int i = 0; i < length; i++){
                sb.append(chars_List.charAt(r.nextInt(chars_List.length())));
            }

            this.TransDesc = sb.toString();
        }

        public String toString(){
            StringBuilder sb = new StringBuilder();
            sb.append(TransID + "," + CustID + "," + TransTotal + "," + TransNumItems + "," + TransDesc);
            return sb.toString();
        }
    }
}
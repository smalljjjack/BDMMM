import java.io.*;
import java.util.Random.*;
import java.util.*;


public class Customers{
    public static void main(String[] args) throws Exception{
        File output = new File("Customers");
        FileOutputStream fs = new FileOutputStream(output);
        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fs));
        bw.write("ID,Name,Age,CountryCode,Salary");
        bw.newLine();
        for(int i = 1; i <= 50000; i++){
            Customer t = new Customer(i);            
            bw.write(t.toString());
            bw.newLine();
        }
        bw.flush();
        bw.close();
    }

    public static class Customer{
        private final String chars_List = 
        "zxcvbnmasdfghjklqwertyuiopZXCVBNMASDFGHJKLQWERTYUIOP1234567890"; 
        private Random r = new Random();

        public int ID;
        public String Name;
        public int Age;
        public int CountryCode;
        public float Salary;

        public Customer(int ID){
            this.ID = ID;
            this.Age = r.nextInt(60)+10;
            this.CountryCode = r.nextInt(10)+1;
            this.Salary = r.nextFloat()*9900+100;
            
            StringBuilder sb = new StringBuilder();
            int length = r.nextInt(10)+10;

            for(int i = 0; i < length; i++){
                sb.append(chars_List.charAt(r.nextInt(chars_List.length())));
            }
            this.Name = sb.toString();
        }

        public String toString(){
            StringBuilder sb = new StringBuilder();
            sb.append(ID + "," + Name + "," + Age + "," + CountryCode + "," + Salary);
            return sb.toString();
        }
    }
}
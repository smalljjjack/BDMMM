import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

public class Dataset_P {

        private int generate_coordinate(){
            Random random = new Random();
            return random.nextInt(10000) + 1;
        }
        public static void main(String[] args) {
            // TODO Auto-generated method stub
            Dataset_P d = new Dataset_P();
            BufferedWriter bw = null;
            FileWriter fw = null;

            try {
                StringBuilder content;
                fw = new FileWriter("./input/Dataset_P.txt");
                bw = new BufferedWriter(fw);
                for (int i = 1; i <= 10000000; i++) {
                    content = new StringBuilder();
                    content.append("<" + d.generate_coordinate() + "," + d.generate_coordinate() + ">");
                    bw.write(content.toString());
                    bw.newLine();
                }

                System.out.println("Done");

            } catch (IOException e) {

                e.printStackTrace();

            } finally {

                try {

                    if (bw != null)
                        bw.close();

                    if (fw != null)
                        fw.close();

                } catch (IOException ex) {

                    ex.printStackTrace();

                }

            }

        }
    }


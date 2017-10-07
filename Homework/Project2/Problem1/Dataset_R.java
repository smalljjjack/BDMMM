import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

public class Dataset_R {
    private int x_top_right, y_top_right;
    private int x_bottom_left, y_bottom_left;

    private String generate_top_left_coordinate(){
        StringBuilder result = new StringBuilder();
        Random random = new Random();
        int y = random.nextInt(10000 - y_bottom_left) + y_bottom_left + 1; // avoid generate the same coordinate as y_bottom_left
        y_top_right = y;
        result.append("<" + x_bottom_left + ", " + y + ">");
        return result.toString();
    }

    private String generate_bottom_right_coordinate(){
        StringBuilder result = new StringBuilder();
        Random random = new Random();
        int x = random.nextInt(10000 - x_bottom_left) + x_bottom_left + 1; // avoid generate the same coordinate as x_bottom_left
        x_top_right = x;
        int y = random.nextInt(y_bottom_left) + 1;
        result.append("<" + x + ", " + y + ">");
        return result.toString();
    }

    private String generate_bottom_left_coordinate(){
        StringBuilder result = new StringBuilder();
        Random random = new Random();
        int x = random.nextInt(9999) + 1;
        int y = random.nextInt(9999) + 1;
        x_bottom_left = x;
        y_bottom_left = y;
        result.append("<" + x + ", " + y + ">");
        return result.toString();
    }
    public static void main(String[] args) {
        // TODO Auto-generated method stub

        BufferedWriter bw = null;
        FileWriter fw = null;

        try {
            StringBuilder content;
            fw = new FileWriter("./input/Dataset_R.txt");
            bw = new BufferedWriter(fw);
            for (int i = 1; i <= 500000; i++) {
                Dataset_R d = new Dataset_R();
                content = new StringBuilder();
                String top_left = d.generate_top_left_coordinate(); // randomly generate the top left coordinate
                content.append("<" + 'r' + i + ",");
                String bottom_left = d.generate_bottom_left_coordinate();
                d.generate_bottom_right_coordinate();
                d.generate_top_left_coordinate();
                content.append(d.x_bottom_left + "," + d.y_bottom_left + "," + d.x_top_right + "," + d.y_top_right + ">");
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


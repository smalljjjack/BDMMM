import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

public class data_gen{
	public static void main(String args[]) throws IOException{
		Random rand_x= new Random();
		Random rand_y= new Random();
		int x,y=0;
		FileWriter points_out=new FileWriter("/home/kavin/College_Work/DS503/Project2/kmeans/points", true);
		File points_file=new File("/home/kavin/College_Work/DS503/Project2/kmeans/points");
		while((points_file.length()/1048576)<100){
			x=rand_x.nextInt(10000);
			y=rand_y.nextInt(10000);
			points_out.append(x+","+y+"\n");
		}
		points_out.close();
	}
}
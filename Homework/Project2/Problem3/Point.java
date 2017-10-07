import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Writable;

public class Point implements Writable{
	DoubleWritable x=new DoubleWritable(0);
	DoubleWritable y=new DoubleWritable(0);
	
	public Point(){
		Random rand_x = new Random();
		Random rand_y = new Random();
		this.x=new DoubleWritable(rand_x.nextInt(10000));
		this.y=new DoubleWritable(rand_y.nextInt(10000));
	}
	public Point(String coords){
		String[] splits=coords.split(",");
		this.x.set(Double.parseDouble(splits[0]));
		this.y.set(Double.parseDouble(splits[1]));
	}
	public Point(double param_x, double param_y){
		this.x.set(param_x);
		this.y.set(param_y);
	}
	public Point(int param_x, int param_y){
		this.x.set(param_x);
		this.y.set(param_y);
	}
	
	public double getX(){
		return this.x.get();
	}
	public double getY(){
		return this.y.get();
	}
	public String toString(){
		String point_str=this.x.get()+","+this.y.get();
		return point_str;
	}
	public  double getDistance(Point pt){
		return Math.sqrt(Math.pow((pt.x.get()-this.x.get()),2)+Math.pow((pt.y.get()-this.y.get()),2));
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		x.readFields(in);
		y.readFields(in);
		
	}
	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		x.write(out);
		y.write(out);
	}
}
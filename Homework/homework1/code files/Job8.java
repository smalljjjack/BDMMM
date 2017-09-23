import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;
import java.util.Iterator;
import java.util.*;

public class Job8{
	public static class Job8Mapper extends Mapper<Object, Text, Text, Text>{
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		  String[] friendData = value.toString().split(",");
		  String userId = friendData[2];
	      context.write(new Text(userId), new Text("1"));
//Map -> key:friendID value :1  
	    }   
	}

	public static class Job8reducer extends Reducer<Text, Text, Text, Text>{
		private static Map<String, Integer> map = new HashMap();
		
		public void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
			int count = 0;
			for(Text val : value) count++;
			map.put(key.toString(), count);
		}
//used map to store how many people list a specific user as friends. IF a user was followed by more person, he/she is more popular.

		public void cleanup(Context context) throws IOException, InterruptedException{
			double sum = 0;
			for(String str : map.keySet()) sum+=map.get(str);

			int average = (int) (sum/map.keySet().size());
// average represent the average number of friends of user has.
			
			for(String str : map.keySet()){
				if(map.get(str) > average){
					String outvalue = String.format("%s, %s, %s", "User: "+str, "has "+map.get(str)+" friends" ,"Average leve is"+average);
					context.write(new Text(outvalue), null);
				}
			}
			
		}
	}

	public static void main(String[] args)throws Exception{
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Job 8");
		job.setJarByClass(Job8.class);
	    job.setMapperClass(Job8Mapper .class);
	    job.setReducerClass(Job8reducer.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    job.setNumReduceTasks(1);
	    FileInputFormat.addInputPath(job, new Path(args[0]));
    	FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}

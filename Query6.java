package samp;

import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Query6 {

	public static class FriendsQ6 extends Mapper<Object, Text, Text, Text> {

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] parts = value.toString().split(",");
			context.write(new Text(parts[1]), new Text("myfriend," + parts[2]));
		}
	}

	public static class myPageQ6 extends Mapper<Object, Text, Text, Text> {

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] parts = value.toString().split(",");
			context.write(new Text(parts[0]), new Text("name," + parts[1]));
		}
	}

	public static class AccessLogQ6 extends Mapper<Object, Text, Text, Text> {

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] parts = value.toString().split(",");
			context.write(new Text(parts[1]), new Text("whatpage," + parts[2]));
		}
	}

	public static class IntSumReducer extends Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			List<Integer> arr = new ArrayList<Integer>();
			String name = "";
			for (Text t : values) {
				String[] str = t.toString().split(",");
				if (str[0].equalsIgnoreCase("whatpage")) {
					arr.add(Integer.parseInt(str[1]));
				} else if (str[0].equalsIgnoreCase("name")) {
					name = str[1];
				}
			}

			for (Text t : values) {
				String[] str1 = t.toString().split(",");
				if (str1[0].equalsIgnoreCase("myFriend"))
					;
				{
					arr.remove(Integer.parseInt(str1[1]));
				}
				if (str1[0].equalsIgnoreCase("name")) {
					name = str1[1];
				}
			}
			int arrSize = arr.size();
			if(!(name.equalsIgnoreCase("")))
			{
			context.write(new Text(name), new Text("user has never accessed " + arrSize + " users"));
			}
		}

	}

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		if (args.length != 4) {
			System.err.println(
					"Please enter input and output path.\n1.InputFile1\t 2.InputFile2\t 3.InputFile3\t 4.Output Path");
			System.exit(2);
		}
		Job job = new Job(conf, "JobForQuery6");
		job.setJarByClass(Query6.class);
		MultipleInputs.addInputPath(job, new Path(args[2]), TextInputFormat.class, AccessLogQ6.class);
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, FriendsQ6.class);
		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, myPageQ6.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setNumReduceTasks(1);
		job.setOutputValueClass(Text.class);
		FileOutputFormat.setOutputPath(job, new Path(args[3]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

}
package samp;

import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Query5 {

	public static class myPageQ5 extends Mapper<Object, Text, Text, Text> {

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] parts = value.toString().split(",");
			context.write(new Text(parts[0]), new Text("name," + parts[1]));
		}
	}

	public static class AccessLogQ5 extends Mapper<Object, Text, Text, Text> {

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] parts = value.toString().split(",");
			context.write(new Text(parts[1]), new Text("whatpage," + parts[2]));
		}
	}

	public static class IntSumReducer extends Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			if (!(key.toString().equalsIgnoreCase(""))) {
				Set<Integer> uniqSet = new HashSet<Integer>();
				String name = "";
				int sum = 0;

				for (Text t : values) {
					String str[] = t.toString().split(",");
					if (str[0].equalsIgnoreCase("whatpage")) {
						sum = sum + 1;
						uniqSet.add(Integer.parseInt(str[1]));
					} else if (str[0].equalsIgnoreCase("name")) {
						name =str[1];
					}

				}
				 String finalOutput = "total access:" + sum + "\tUnique access:" + uniqSet.size();
				if(!(name.equalsIgnoreCase("")))
				{
				context.write(new Text(name), new Text(finalOutput));
				}
			}
		}

	}

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		if (args.length != 3) {
			System.err.println("Please enter input and output path.\n1.InputFile1\t 2.InputFile2\t 3.Output Path");
			System.exit(2);
		}
		Job job = new Job(conf, "JobForQuery5");
		job.setJarByClass(Query5.class);
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, AccessLogQ5.class);
		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, myPageQ5.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setNumReduceTasks(1);
		job.setOutputValueClass(Text.class);
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

}
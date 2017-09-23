package samp;

import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Query4 {

	public static class MyPageQ4 extends Mapper<Object, Text, Text, Text> {

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] parts = value.toString().split(",");
			context.write(new Text(parts[0]), new Text("name," + parts[1]));
		}
	}

	public static class FriendsQ4 extends Mapper<Object, Text, Text, Text> {

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] str = value.toString().split(",");
			context.write(new Text(str[2]), new Text("count," + 1));

		}
	}

	public static class IntSumReducer extends Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
			String name = "";
			for (Text t : values) {
				String[] part = t.toString().split(",");

				if (part[0].equalsIgnoreCase("name")) {
					name = part[1];
				} else
					sum = sum + 1;
			}
			String totalSum = String.format("%d", sum);
			if(!(name.equalsIgnoreCase("")))
			{
			context.write(new Text(name), new Text(totalSum+" people listed him as a friend"));
			}
		}

	}

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		if (args.length != 3) {
			System.err.println("Please enter input and output path.\n1.InputFile1\t 2.InputFile2\t 3.Output Path");
		}
		Job job = new Job(conf, "JobForQuery4");
		job.setJarByClass(Query4.class);
		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, MyPageQ4.class);
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, FriendsQ4.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setNumReduceTasks(1);
		job.setOutputValueClass(Text.class);
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

}
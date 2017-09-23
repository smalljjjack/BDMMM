package samp;

import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Query7 {

	public static class MyPageQ7 extends Mapper<Object, Text, Text, Text> {

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] parts = value.toString().split(",");
			context.write(new Text(parts[0]), new Text("name," + parts[1]));
		}
	}

	public static class AccessQ7 extends Mapper<Object, Text, Text, Text> {

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] nm = value.toString().split(",");
			context.write(new Text(nm[1]), new Text("date," + nm[4]));

		}
	}

	public static class IntSumReducer extends Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			String name = "";
			List<Integer> date = new ArrayList<Integer>();
			int dateDiff = 0;

			for (Text t : values) {
				String[] part = t.toString().split(",");

				if (part[0].equalsIgnoreCase("name")) {
					name = part[1];
				} else if (part[0].equalsIgnoreCase("date")) {
					date.add(Integer.parseInt(part[1]));
				}
			}
			if (!(name.equalsIgnoreCase(""))) {
				if (!(date.isEmpty())) {
					Collections.sort(date);
					dateDiff = date.get(date.size() - 1) - date.get(0);
				}
				if (dateDiff < 20000) {

					context.write(new Text(name),
							new Text("Never accessed the account after " + dateDiff + " days of creating the account"));
				}
			}

		}

	}

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		if (args.length != 3) {
			System.err.println("Please enter input and output path.\n1.InputFile1\t 2.InputFile2 3.Output Path");
			System.exit(2);
		}
		Job job = new Job(conf, "InactiveUser");
		job.setJarByClass(Query7.class);

		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, MyPageQ7.class);
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, AccessQ7.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setNumReduceTasks(1);
		job.setOutputValueClass(Text.class);
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

}
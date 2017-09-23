package samp;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Query1 {

	public static class MyPageQ1 extends Mapper<Object, Text, Text, Text> {
		private Text name = new Text();
		private Text hobby = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String myNationality = context.getConfiguration().get("myNationality");
			String[] it = value.toString().split(",");
			if (it[2].equalsIgnoreCase(myNationality)) {
				name.set(it[1]);
				hobby.set(it[4]);
				context.write(name, hobby);
			}
		}
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

		Configuration conf = new Configuration();

		if (args.length != 3) {
			System.err.println(
					"Please enter the your nationality as the first argument.\n1.Nationality, 2.InputFile1, 3.Output Path");
			System.exit(2);
		}
		conf.set("myNationality", args[0]);
		Job job = new Job(conf, "JobForQuery1");
		job.setJarByClass(Query1.class);
		job.setMapperClass(MyPageQ1.class);
		job.setOutputKeyClass(Text.class);
		job.setNumReduceTasks(1);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

}

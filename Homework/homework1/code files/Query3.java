/**
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package samp;

import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.StringTokenizer;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Query3 {

	public static class MyPageQ3 extends Mapper<Object, Text, Text, Text> {

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] parts = value.toString().split(",");
			context.write(new Text(parts[0]), new Text("name," + parts[1]));
		}
	}

	public static class AccessLogQ3 extends Mapper<Object, Text, Text, Text> {

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] nm = value.toString().split(",");
			context.write(new Text(nm[2]), new Text("count," + 1));

		}
	}

	public static class IntSumReducer extends Reducer<Text, Text, Text, Text> {
		Map<Text, Text> map = new HashMap<Text,Text>();

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
			String name = "";
			for (Text t : values) {
				String[] part = t.toString().split(",");

				if (part[0].equalsIgnoreCase("name")) {
					name = part[1];
				} else if (part[0].equalsIgnoreCase("count"))
					sum = sum + 1;
			}
			String totalSum = String.format("%d", sum);
			map.put(new Text(name), new Text(totalSum)); // name and occurence
		}

		protected void cleanup(Context context) throws IOException, InterruptedException {

			List<Entry<Text, Text>> sortedMap = new ArrayList<Entry<Text, Text>>(map.entrySet());
			Collections.sort(sortedMap, new Comparator<Entry<Text, Text>>() {

				public int compare(Entry<Text, Text> e1, Entry<Text, Text> e2) {
					//return e2.getValue().compareTo(e1.getValue());
					return Integer.compare(Integer.parseInt(e2.getValue().toString()), Integer.parseInt(e1.getValue().toString()));
				}
			});

			int counter = 0;
			for (Entry<Text, Text> k1 : sortedMap) {
				if (counter >= 10)
					break;
				context.write(k1.getKey(), new Text(k1.getValue()));
				counter++;
			}
		}

	}

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		if (args.length != 3) {
			System.err.println(
					"Please enter input and output path. Usage: wordcount <HDFS input file> <HDFS output file>");
			System.exit(2);
		}
		Job job = new Job(conf, "Find Friends");
		job.setJarByClass(Query3.class);
		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, MyPageQ3.class);
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, AccessLogQ3.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setNumReduceTasks(1);
		job.setOutputValueClass(Text.class);
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

}
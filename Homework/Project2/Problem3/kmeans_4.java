import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class kmeans_4 {

	//Creates mapper class that reads the points and maps them to a centroid based on the centroids from the file 
	public static class CentroidMapper extends
			Mapper<Object, Text, IntWritable, Point> {
		HashMap<Integer, Point> centroids_map = new HashMap<Integer, Point>();

		//Run in a mapper once to read the centroids from a file in the cache. The file is reasonably small so it is shared to all mappers.
		//Each centroid has a cluster they are associated with. The centroids and their cluster numbers are entered in a hashmap with the cluster number as the key.
		//Centroids are stored as custom object type Point.
		public void setup(Context context) throws IOException {
			URI[] cache_files = context.getCacheFiles();
			Path centroid_path = new Path(cache_files[0]);
			Configuration conf = context.getConfiguration();
			FileSystem hdfs = null;
			try {
				hdfs = FileSystem.get(new URI("hdfs://localhost:8020"), conf);
			} catch (URISyntaxException e) {
				e.printStackTrace();
			}
			BufferedReader buffer_reader = new BufferedReader(
					new InputStreamReader(hdfs.open(centroid_path)));
			String line = null;
			while ((line = buffer_reader.readLine()) != null) {
				String[] centroid = line.split(":");
				centroids_map.put(Integer.parseInt(centroid[0]), new Point(
						centroid[1]));
			}
			buffer_reader.close();

		}

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			Point point = new Point(value.toString());
			IntWritable centroid_key = new IntWritable(0);
			DoubleWritable distance = new DoubleWritable();
			int closest_centroid = 0;
			double least_distance = -1.0;
			for (Entry<Integer, Point> entry : centroids_map.entrySet()) {
				int centroid_num = entry.getKey();
				Point centroid = entry.getValue();
				double dist = centroid.getDistance(point);
				if (least_distance == -1.0) {
					least_distance = dist;
					closest_centroid = centroid_num;
				} else if (dist < least_distance) {
					least_distance = dist;
					closest_centroid = centroid_num;
				}
			}
			centroid_key.set(closest_centroid);
			distance.set(least_distance);
			context.write(centroid_key, point);
		}
	}

	public static class CentroidReducer extends
			Reducer<IntWritable, Point, Text, Text> {
		HashMap<Integer, Point> centroids_map = new HashMap<Integer, Point>();
		HashMap<Integer, Point> new_centroids = new HashMap<Integer, Point>();

		public void setup(Context context) throws IOException {
			URI[] cache_files = context.getCacheFiles();
			Path centroid_path = new Path(cache_files[0]);
			Configuration conf = context.getConfiguration();
			FileSystem hdfs = null;
			try {
				hdfs = FileSystem.get(new URI("hdfs://localhost:8020"), conf);
			} catch (URISyntaxException e) {
				e.printStackTrace();
			}
			BufferedReader buffer_reader = new BufferedReader(
					new InputStreamReader(hdfs.open(centroid_path)));
			String line = null;
			while ((line = buffer_reader.readLine()) != null) {
				String[] centroid = line.split(":");
				centroids_map.put(Integer.parseInt(centroid[0]), new Point(
						centroid[1]));
			}
			buffer_reader.close();
			
		}

		public void reduce(IntWritable key, Iterable<Point> values,
				Context context) throws IOException, InterruptedException {
			double sum_x = 0;
			double sum_y = 0;
			double cluster_size = 0;
			Point new_centroid = new Point();
			for (Point val : values) {
				sum_x += val.getX();
				sum_y += val.getY();
				cluster_size++;
			}
			if (cluster_size > 0) {
				double cent_x = sum_x / cluster_size;
				double cent_y = sum_y / cluster_size;
				new_centroid = new Point(cent_x, cent_y);
				new_centroids.put(key.get(), new_centroid);
			}
		}

		public void cleanup(Context context) throws IOException,
				InterruptedException {
			Configuration conf = context.getConfiguration();
			FileSystem hdfs = null;
			Text iter_count = new Text();
			Text flag = new Text();
			flag.set("True");
			iter_count.set(conf.get("iterationCount").toString());

			try {
				hdfs = FileSystem.newInstance(
						new URI("hdfs://localhost:8020/"), conf);
			} catch (URISyntaxException e) {
				e.printStackTrace();
			}
			BufferedWriter buffer_writer = new BufferedWriter(
					new OutputStreamWriter(
							hdfs.create(new Path(conf.get("centroid_path")))));
			for (Entry<Integer, Point> entry : new_centroids.entrySet()) {
				int centroid_num = entry.getKey();
				Point centroid = entry.getValue();
				double dist = centroid.getDistance(centroids_map
						.get(centroid_num));
				buffer_writer.write(centroid_num + ":" + centroid.toString()
						+ "," + conf.get("iterationCount") + "\n");
				if (dist > Double.parseDouble(conf.get("convergenceThreshold")))
					flag.set("False");
			}
			buffer_writer.close();
			context.write(iter_count, flag);
			hdfs.close();

		}
	}

	//Creates file with specified number of random centroids in the specified path
		public static void createCentroids(int num, Path outpath)
				throws IOException, URISyntaxException {
			Configuration configuration = new Configuration();
			FileSystem hdfs = FileSystem.get(new URI("hdfs://localhost:8020"),
					configuration);
			OutputStream out_stream = hdfs.create(outpath);
			BufferedWriter buffer_writer = new BufferedWriter(
					new OutputStreamWriter(out_stream, "UTF-8"));
			int centroid_id = 1;
			while (num > 0) {
				Point centroid = new Point();
				buffer_writer.write(centroid_id + ":" + centroid.toString() + "\n");
				num--;
				centroid_id++;
			}
			buffer_writer.close();
			hdfs.close();

		}

	public static void main(String[] args) throws Exception {
		if(args.length<5)
			System.out.println("Enter input parameters *input file* *output folder* *number of clusters* *maximum number of iterations* *threshold for convergence*");
		
		
		int k = Integer.parseInt(args[2]);
		int r_iter = Integer.parseInt(args[3]);
		double convergence_threshold = Double.parseDouble(args[4]);

		Configuration conf = new Configuration();
		conf.set("fs.hdfs.impl",
				org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
		conf.set("fs.file.impl",
				org.apache.hadoop.fs.LocalFileSystem.class.getName());
		conf.set("mapreduce.output.textoutputformat.separator", ":");
		conf.set("convergenceThreshold", "" + convergence_threshold);
		Path centroids_filepath = new Path(args[1]+"/centroids");
		conf.set("centroid_path", centroids_filepath.toString());
		createCentroids(k, centroids_filepath);
		int iter_num = 1;
		long startTime = System.currentTimeMillis();
		FileSystem hdfs = null;
		try {
			hdfs = FileSystem.newInstance(new URI("hdfs://localhost:8020/"),
					conf);
		} catch (URISyntaxException e) {
			e.printStackTrace();
		}
		while (iter_num <= r_iter) {
			conf.set("iterationCount", "" + iter_num);
			Job job = Job.getInstance(conf, "kmeans");
			job.addCacheFile(centroids_filepath.toUri());
			job.setJarByClass(kmeans_4.class);
			job.setMapperClass(CentroidMapper.class);
			job.setReducerClass(CentroidReducer.class);
			//job.setNumReduceTasks(1);
			job.setMapOutputKeyClass(IntWritable.class);
			job.setMapOutputValueClass(Point.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			FileInputFormat
					.addInputPath(
							job,
							new Path(args[0]));
			FileOutputFormat
					.setOutputPath(
							job,
							new Path(args[1]+"/centroids_tmp"));
			if (job.waitForCompletion(true)) {

				BufferedReader buffer_reader = new BufferedReader(
						new InputStreamReader(
								hdfs.open(new Path(args[1]+"/centroids_tmp/part-r-00000"))));
				String line = null;
				boolean convergence_flag = false;
				while ((line = buffer_reader.readLine()) != null) {
					String[] splits = line.split(":");
					if (splits[1].trim().contains("True"))
						convergence_flag = true;
				}
				if (convergence_flag)
					break;
				hdfs.delete(
						new Path(args[1]+"/centroids_tmp"),
						true);
			}
			iter_num++;
		}
		hdfs.close();
		long endTime = System.currentTimeMillis();
		System.out.println((endTime - startTime) / 1000);
	}
}

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
import org.apache.hadoop.fs.FileStatus;
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


public class kmeans_5a {

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
			Reducer<IntWritable, Point, IntWritable, Point> {
		
		//writes the cluster number and new centroid
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
			}
			context.write(key, new_centroid);
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


		//After each iteration, convergence is checked, if convergence is reached, output centroid file is written to indicate that
	public static void main(String[] args) throws Exception {
		if(args.length<5)
			System.out.println("Enter input parameters *input file* *output folder* *number of clusters* *maximum number of iterations* *threshold for convergence* [*combiner*]");
		boolean combiner_flag=false;
		if(args.length==6){
			if(args[5].toLowerCase().trim()==("combiner"))
				combiner_flag=true;
		}
		int k = Integer.parseInt(args[2]);
		int r_iter = Integer.parseInt(args[3]);
		double convergence_threshold = Double.parseDouble(args[4]);

		HashMap<Integer, Point> old_centroids = new HashMap<Integer, Point>();
		HashMap<Integer, Point> new_centroids = new HashMap<Integer, Point>();

		Configuration conf = new Configuration();
		conf.set("fs.hdfs.impl",
				org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
		conf.set("fs.file.impl",
				org.apache.hadoop.fs.LocalFileSystem.class.getName());
		conf.set("mapreduce.output.textoutputformat.separator", ":");
		Path centroids_filepath = new Path(args[1]+"/centroids");
		createCentroids(k, centroids_filepath);
		int iter_num = 1;
		long startTime = System.currentTimeMillis();
		while (iter_num <= r_iter) {
			Job job = Job.getInstance(conf, "kmeans");
			job.addCacheFile(centroids_filepath.toUri());
			job.setJarByClass(kmeans_5a.class);
			job.setMapperClass(CentroidMapper.class);
			if(combiner_flag){
				job.setCombinerClass(CentroidReducer.class);
			}
			job.setReducerClass(CentroidReducer.class);
			// job.setNumReduceTasks(5);
			job.setMapOutputKeyClass(IntWritable.class);
			job.setMapOutputValueClass(Point.class);
			job.setOutputKeyClass(IntWritable.class);
			job.setOutputValueClass(Point.class);
			FileInputFormat
					.addInputPath(
							job,
							new Path(args[0]));
			FileOutputFormat
					.setOutputPath(
							job,
							new Path(args[1]+"/centroids_tmp"));
			if (job.waitForCompletion(true)) {

				FileSystem hdfs = null;
				try {
					hdfs = FileSystem.newInstance(new URI(
							"hdfs://localhost:8020/"), conf);
				} catch (URISyntaxException e) {
					e.printStackTrace();
				}
				FileStatus[] fileStatus = hdfs
						.listStatus(new Path(args[1]+"/centroids_tmp"));
				BufferedReader buffer_reader = new BufferedReader(
						new InputStreamReader(
								hdfs.open(centroids_filepath)));
				String line = null;
				while ((line = buffer_reader.readLine()) != null) {
					String[] centroid = line.split(":");
					old_centroids.put(Integer.parseInt(centroid[0]), new Point(
							centroid[1]));
				}
				buffer_reader.close();
				BufferedWriter buffer_writer = new BufferedWriter(
						new OutputStreamWriter(
								hdfs.create(centroids_filepath)));
				for (FileStatus status : fileStatus) {
					buffer_reader = new BufferedReader(new InputStreamReader(
							hdfs.open(status.getPath())));
					line = null;
					Point centroid = null;
					while ((line = buffer_reader.readLine()) != null) {
						String[] splits = line.split(":");
						centroid = new Point(splits[1]);
						new_centroids
								.put(Integer.parseInt(splits[0]), centroid);
						double dist = centroid.getDistance(old_centroids
								.get(Integer.parseInt(splits[0])));
						String convergence = "False";
						if (dist < convergence_threshold)
							convergence = "True";
						buffer_writer.write(splits[0] + ":"
								+ centroid.toString() + "," + convergence
								+ "\n");
					}
				}
				int convergence_flag = 1;
				for (Entry<Integer, Point> entry : new_centroids.entrySet()) {
					int centroid_num = entry.getKey();
					Point centroid = entry.getValue();
					double dist = centroid.getDistance(old_centroids
							.get(centroid_num));
					if (dist > convergence_threshold)
						convergence_flag *= 0;
				}
				buffer_reader.close();
				buffer_writer.close();
				hdfs.delete(
						new Path(args[1]+"/centroids_tmp"),
						true);
				hdfs.close();
				if (convergence_flag == 1)
					break;
			}
			iter_num++;
		}
		long endTime = System.currentTimeMillis();
		System.out.println((endTime - startTime) / 1000);
	}
}

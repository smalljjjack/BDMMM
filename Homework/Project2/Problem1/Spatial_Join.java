import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Spatial_Join {
    private static Map<String, String> coordinate = new HashMap<String, String>();
    static int count = 0;
    public static class SpatialJoinMapper extends Mapper<Object, Text, Text, Text>{

//        private final static IntWritable one = new IntWritable(1);
//        private Text word = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] str = line.substring(1, line.length() - 1).split(",");
            context.write(new Text(str[0]), new Text(str[1]));
        }
        protected void setup(Mapper<Object, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {
            // loading user map in context
            loadUserInMemory(context);
        }

        private void loadUserInMemory(Mapper<Object, Text, Text, Text>.Context context) throws IOException {
            Path[] uris = DistributedCache.getLocalCacheFiles(context
                    .getConfiguration());
            // user.log is in distributed cache

//            if(input != null) {
//                coordinate.put("user", input.substring(2, input.length() - 1));
//            } else {
                try (BufferedReader br = new BufferedReader(new FileReader("./input/Dataset_R.txt"))) {
                    String line;
                    while ((line = br.readLine()) != null) {
//                    System.out.println(line.toString());
//                    System.out.println(line);
                        String[] str = line.substring(1, line.length() - 1).split(",");
                        StringBuilder tmp = new StringBuilder();
                        tmp.append(str[1] + ',' + str[2] + ',' + str[3] + ',' + str[4]);
                        coordinate.put(str[0], tmp.toString());
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

//        }
    }

    public static class SpatialJoinReducer extends Reducer<Text, Text, Text, NullWritable>{
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            count++;
            for(Text text : values) {
                int x = Integer.valueOf(key.toString());
                int y = Integer.valueOf(text.toString());
                String tmp = isContained(context, x, y);
                if(tmp.length() > 0) {
                    String input = context.getConfiguration().get("input");
                    System.out.println(input);
                    String[] str = input.substring(2, input.length() - 1).split(",");
                    int x_left = Integer.valueOf(str[0]);
                    int x_right = Integer.valueOf(str[2]);
                    int y_low = Integer.valueOf(str[1]);
                    int y_high = Integer.valueOf(str[3]);
                    System.out.println(x_left);
                    System.out.println(x_right);
                    System.out.println(y_low);
                    System.out.println(y_high);
                    if(x >= x_left && x <= x_right && y >= y_low && y <= y_high)
                        context.write(new Text(tmp), NullWritable.get());
                }

            }
        }
        private static String isContained(Context context, int x_coordinate, int y_coordinate) throws IOException, InterruptedException{
            for(Map.Entry<String, String> entry : coordinate.entrySet()) {
                String[] str = entry.getValue().split(",");
                int x_left = Integer.valueOf(str[0]);
                int x_right = Integer.valueOf(str[2]);
                int y_low = Integer.valueOf(str[1]);
                int y_high = Integer.valueOf(str[3]);

                if(x_coordinate >= x_left && x_coordinate <= x_right && y_coordinate >= y_low && y_coordinate <= y_high) {
                    StringBuilder res = new StringBuilder();
                    res.append("<" + entry.getKey() + ',' + "(" + x_coordinate + ',' + y_coordinate + ')' + '>');
                    return res.toString();
                } else return "";
            }
            return "";
        }
    }



    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("input", "W(7000,1100,8000,1500)");
        if (args.length != 2) {
            System.err.println("Usage: taska <HDFS input file> <HDFS output file>");
            System.exit(2);
        }
        Job job = new Job(conf, "TaskA");
        job.setJarByClass(Spatial_Join.class);
        job.setMapperClass(SpatialJoinMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
    job.setReducerClass(SpatialJoinReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setNumReduceTasks(1);
        job.setOutputValueClass(NullWritable.class);

//        MultipleInputs.addInputPath(job, p1, TextInputFormat.class, SpatialJoinMapper.class);
//        MultipleInputs.addInputPath(job,p2, TextInputFormat.class, SpatialJoin2Mapper.class);
        DistributedCache.addCacheFile(new URI("./input/Dataset_R.txt"), conf);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        //Performance Evaluation
        long start = new Date().getTime();
        boolean status = job.waitForCompletion(true);
        long end = new Date().getTime();
        System.out.println("Job took "+(end-start) + "milliseconds");
        System.exit(job.waitForCompletion(true) ? 0 : 1);
        System.out.println(Spatial_Join.count);
    }
}


import java.io.IOException;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

// Mapper Class
public class MatrixMultiply {

    public static class MapClass extends Mapper<LongWritable, Text, Text, Text> {
        private Text outputKey = new Text();
        private Text outputValue = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // Each line: MatrixName i j value
            String[] parts = value.toString().split("\t");
            String matrixName = parts[0];
            int i = Integer.parseInt(parts[1]);
            int j = Integer.parseInt(parts[2]);
            int val = Integer.parseInt(parts[3]);

            if (matrixName.equals("M")) {
                // Emit (k, (M,i,value))
                for (int k = 0; k < 3; k++) { // 3 = column size of M or row size of N
                    outputKey.set(i + "," + k);
                    outputValue.set("M," + j + "," + val);
                    context.write(outputKey, outputValue);
                }
            } else {
                // Matrix N
                for (int k = 0; k < 3; k++) {
                    outputKey.set(k + "," + j);
                    outputValue.set("N," + i + "," + val);
                    context.write(outputKey, outputValue);
                }
            }
        }
    }

    // Reducer Class
    public static class ReduceClass extends Reducer<Text, Text, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Map<Integer, Integer> mapM = new HashMap<>();
            Map<Integer, Integer> mapN = new HashMap<>();

            for (Text val : values) {
                String[] parts = val.toString().split(",");
                if (parts[0].equals("M")) {
                    mapM.put(Integer.parseInt(parts[1]), Integer.parseInt(parts[2]));
                } else {
                    mapN.put(Integer.parseInt(parts[1]), Integer.parseInt(parts[2]));
                }
            }

            int sum = 0;
            for (int mKey : mapM.keySet()) {
                if (mapN.containsKey(mKey)) {
                    sum += mapM.get(mKey) * mapN.get(mKey);
                }
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    // Driver Code
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Matrix Multiply");
        job.setJarByClass(MatrixMultiply.class);
        job.setMapperClass(MapClass.class);
        job.setReducerClass(ReduceClass.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

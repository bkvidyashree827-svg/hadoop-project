import java.io.IOException;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MatrixVectorMultiply {

    // ============================
    // Mapper 1: Process Matrix and Vector
    // ============================
    public static class MapClass extends Mapper<LongWritable, Text, IntWritable, Text> {
        private IntWritable outputKey = new IntWritable();
        private Text outputValue = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split("\t");

            if (parts[0].equals("M")) {
                // M row col value
                int row = Integer.parseInt(parts[1]);
                int col = Integer.parseInt(parts[2]);
                double val = Double.parseDouble(parts[3]);
                outputKey.set(col);
                outputValue.set("M," + row + "," + val);
                context.write(outputKey, outputValue);
            } else if (parts[0].equals("V")) {
                // V col 1 value
                int col = Integer.parseInt(parts[1]);
                double val = Double.parseDouble(parts[3]);
                outputKey.set(col);
                outputValue.set("V," + val);
                context.write(outputKey, outputValue);
            }
        }
    }

    // ============================
    // Reducer 1: Multiply Matrix element with Vector element
    // ============================
    public static class ReduceClass extends Reducer<IntWritable, Text, IntWritable, DoubleWritable> {
        public void reduce(IntWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            List<String> matrixList = new ArrayList<>();
            double vectorValue = 0.0;

            for (Text val : values) {
                String[] parts = val.toString().split(",");
                if (parts[0].equals("M")) {
                    matrixList.add(parts[1] + "," + parts[2]);
                } else if (parts[0].equals("V")) {
                    vectorValue = Double.parseDouble(parts[1]);
                }
            }

            for (String m : matrixList) {
                String[] mParts = m.split(",");
                int row = Integer.parseInt(mParts[0]);
                double mVal = Double.parseDouble(mParts[1]);
                context.write(new IntWritable(row), new DoubleWritable(mVal * vectorValue));
            }
        }
    }

    // ============================
    // Mapper 2: Pass-through Mapper for Step 2
    // ============================
    public static class PassThroughMapper extends Mapper<LongWritable, Text, IntWritable, DoubleWritable> {
        private IntWritable outputKey = new IntWritable();
        private DoubleWritable outputValue = new DoubleWritable();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split("\t");
            outputKey.set(Integer.parseInt(parts[0]));
            outputValue.set(Double.parseDouble(parts[1]));
            context.write(outputKey, outputValue);
        }
    }

    // ============================
    // Reducer 2: Sum partial products
    // ============================
    public static class SumReducer extends Reducer<IntWritable, DoubleWritable, IntWritable, DoubleWritable> {
        public void reduce(IntWritable key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {

            double sum = 0;
            for (DoubleWritable val : values) {
                sum += val.get();
            }
            context.write(key, new DoubleWritable(sum));
        }
    }

    // ============================
    // MAIN METHOD
    // ============================
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        // -------- Job 1: Multiply matrix elements with vector --------
        Job job1 = Job.getInstance(conf, "Matrix Vector Multiply - Step 1");
        job1.setJarByClass(MatrixVectorMultiply.class);
        job1.setMapperClass(MapClass.class);
        job1.setReducerClass(ReduceClass.class);
        job1.setMapOutputKeyClass(IntWritable.class);
        job1.setMapOutputValueClass(Text.class);
        job1.setOutputKeyClass(IntWritable.class);
        job1.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1] + "_temp"));

        job1.waitForCompletion(true);

        // -------- Job 2: Sum the results for each row --------
        Job job2 = Job.getInstance(conf, "Matrix Vector Multiply - Step 2");
        job2.setJarByClass(MatrixVectorMultiply.class);
        job2.setMapperClass(PassThroughMapper.class);
        job2.setReducerClass(SumReducer.class);
        job2.setMapOutputKeyClass(IntWritable.class);
        job2.setMapOutputValueClass(DoubleWritable.class);
        job2.setOutputKeyClass(IntWritable.class);
        job2.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(job2, new Path(args[1] + "_temp"));
        FileOutputFormat.setOutputPath(job2, new Path(args[1]));

        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}

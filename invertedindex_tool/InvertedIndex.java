import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class InvertedIndex {

    public static class MapClass extends Mapper<LongWritable, Text, Text, Text> {
        private Text word = new Text();
        private Text fileName = new Text();

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String file = ((FileSplit) context.getInputSplit()).getPath().getName();
            fileName.set(file);
            String line = value.toString().toLowerCase();
            String[] words = line.split("\\W+");
            for (String w : words) {
                if (w.length() > 0) {
                    word.set(w);
                    context.write(word, fileName);
                }
            }
        }
    }

    public static class ReduceClass extends Reducer<Text, Text, Text, Text> {
        private Text result = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            Set<String> uniqueFiles = new HashSet<>();
            for (Text val : values) {
                uniqueFiles.add(val.toString());
            }
            result.set(String.join(",", uniqueFiles));
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Inverted Index");

        job.setJarByClass(InvertedIndex.class);
        job.setMapperClass(MapClass.class);
        job.setReducerClass(ReduceClass.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

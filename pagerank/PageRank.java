import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PageRank {

    public static class PRMapper extends Mapper<LongWritable, Text, Text, Text> {
        // input: node \t rank \t out1,out2,...
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.length() == 0) return;
            String[] parts = line.split("\\t");
            String node = parts[0];
            double rank = 0.0;
            String links = "";
            if (parts.length >= 2) {
                try { rank = Double.parseDouble(parts[1]); } catch(Exception e){}
            }
            if (parts.length >= 3) links = parts[2];

            // emit adjacency list for node so reducer can rebuild graph
            context.write(new Text(node), new Text("LINKS|" + links));

            if (links.length() == 0) {
                // dangling node: no outlinks -> no contributions emitted
                return;
            }

            String[] outs = links.split(",");
            double share = rank / outs.length;
            for (String out : outs) {
                if (out.trim().length() == 0) continue;
                context.write(new Text(out.trim()), new Text("PR|" + Double.toString(share)));
            }
        }
    }

    public static class PRReducer extends Reducer<Text, Text, Text, Text> {
        private static final double DAMPING = 0.85;

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String links = "";
            double sumContrib = 0.0;
            for (Text t : values) {
                String s = t.toString();
                if (s.startsWith("LINKS|")) {
                    links = s.substring(6);
                } else if (s.startsWith("PR|")) {
                    try { sumContrib += Double.parseDouble(s.substring(3)); }
                    catch(Exception e) {}
                }
            }
            double newRank = (1 - DAMPING) + DAMPING * sumContrib;
            context.write(key, new Text(Double.toString(newRank) + "\t" + links));
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: PageRank <input> <output>");
            System.exit(2);
        }
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "pagerank");
        job.setJarByClass(PageRank.class);
        job.setMapperClass(PRMapper.class);
        job.setReducerClass(PRReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

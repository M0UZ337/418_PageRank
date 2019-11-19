import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;


public class PageRank {
    
    public static class prePRMapper extends Mapper<IntWritable, PRNodeWritable, IntWritable, PRNodeWritable> {
        public void map(IntWritable key, PRNodeWritable node, Context context) throws IOException, InterruptedException {
            context.write(key, node);
        }
    }

    public static class prePRReducer extends Reducer<IntWritable, PRNodeWritable, IntWritable, PRNodeWritable> {
        public void reduce(IntWritable key, Iterable<PRNodeWritable> values, Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            long nodeCount = Long.parseLong(conf.get("nodeCount"));
            for (PRNodeWritable node : values){
                PRNodeWritable newNode = new PRNodeWritable(node.getNodeID(), new DoubleWritable(1.0/nodeCount), node.getChildNum(), node.getAdjList());
                context.write(key, newNode);
            }
        }
    }

    public static class FinalMapper extends Mapper<IntWritable, PRNodeWritable, IntWritable, Text> {
        public void map(IntWritable key, PRNodeWritable node, Context context) throws IOException, InterruptedException {
            context.write(key, node.toText());
        }
    }

    public static class FinalReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            
            for (Text value : values){
                context.write(key, value);
            }
        
        }
    }

    public static enum NodeCounter { COUNT };
    public static long nodeCount;

    public static void main(String[] args) throws Exception
    {   
        Configuration preProcessConf = new Configuration();
        Job preProcessJob = Job.getInstance(preProcessConf, "pre process");
        preProcessJob.setJarByClass(PRPreProcess.class);
        preProcessJob.setMapperClass(PRPreProcess.InputParser.class);
        preProcessJob.setMapOutputKeyClass(IntWritable.class);
        preProcessJob.setMapOutputValueClass(MapWritable.class);
        preProcessJob.setReducerClass(PRPreProcess.NodeReducer.class);
        preProcessJob.setOutputKeyClass(IntWritable.class);
        preProcessJob.setOutputValueClass(PRNodeWritable.class);
        preProcessJob.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileInputFormat.addInputPath(preProcessJob, new Path(args[0]));
        FileOutputFormat.setOutputPath(preProcessJob, new Path("/user/hadoop/tmp/preProcess"));

        preProcessJob.waitForCompletion(true);

        nodeCount = preProcessJob.getCounters().findCounter(PageRank.NodeCounter.COUNT).getValue();

        Configuration prePRConf = new Configuration();
        prePRConf.set("nodeCount", Long.toString(nodeCount));
        Job prePRJob = Job.getInstance(prePRConf, "pre PR process");
        prePRJob.setJarByClass(PRPreProcess.class);
        prePRJob.setMapperClass(prePRMapper.class);
        prePRJob.setMapOutputKeyClass(IntWritable.class);
        prePRJob.setMapOutputValueClass(PRNodeWritable.class);
        prePRJob.setInputFormatClass(SequenceFileInputFormat.class);
        prePRJob.setReducerClass(prePRReducer.class);
        prePRJob.setOutputKeyClass(IntWritable.class);
        prePRJob.setOutputValueClass(PRNodeWritable.class);
        prePRJob.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileInputFormat.addInputPath(prePRJob, new Path("/user/hadoop/tmp/preProcess"));
        FileOutputFormat.setOutputPath(prePRJob, new Path("/user/hadoop/tmp/Iteration0"));

        prePRJob.waitForCompletion(true);

        int i = 0;

        Configuration printResultConf = new Configuration();
        printResultConf.set("mapreduce.output.textoutputformat.separator", " ");
        Job printResultJob = Job.getInstance(printResultConf, "print final result");
        printResultJob.setJarByClass(PageRank.class);
        printResultJob.setMapperClass(FinalMapper.class);
        printResultJob.setMapOutputKeyClass(IntWritable.class);
        printResultJob.setMapOutputValueClass(Text.class);
        printResultJob.setInputFormatClass(SequenceFileInputFormat.class);
        printResultJob.setReducerClass(FinalReducer.class);
        printResultJob.setOutputKeyClass(IntWritable.class);
        printResultJob.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(printResultJob, new Path("/user/hadoop/tmp/Iteration" + Integer.toString(i)));
        FileOutputFormat.setOutputPath(printResultJob, new Path(args[1]));
                
        System.exit(printResultJob.waitForCompletion(true) ? 0 : 1);
    }
}
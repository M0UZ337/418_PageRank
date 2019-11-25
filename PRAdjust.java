import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
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

public class PRAdjust {

    public static class PRAdjustMapper extends Mapper<IntWritable, PRNodeWritable, IntWritable, PRNodeWritable> {
        public void map(IntWritable key, PRNodeWritable node, Context context) throws IOException, InterruptedException {
            context.write(key, node);
        }

    }

    public static class PRAdjustReducer extends Reducer<IntWritable, PRNodeWritable, IntWritable, PRNodeWritable> { 
        double alpha;
        double missMass;
        long nodeCount;
        public void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            FileSystem fs = FileSystem.get(conf);
            FSDataInputStream out = fs.open(new Path("/user/hadoop/tmp/currentMass"));
            double currentMass = out.readDouble();
            out.close();
            alpha = Double.parseDouble(conf.get("alpha"));
            missMass = 1 - currentMass;
            nodeCount = Long.parseLong(conf.get("nodeCount"));
        }
        public void reduce(IntWritable key, Iterable<PRNodeWritable> nodes, Context context) throws IOException, InterruptedException {
            
            for(PRNodeWritable node : nodes){
                double prValue = node.getPRValue().get();
                double xPR = node.getXPR().get();
                double adjustedPR = (alpha / nodeCount) + (1.0 - alpha) * ((missMass / nodeCount) + prValue);
                if(!(Math.abs(adjustedPR - xPR) < 0.00001)){
                    // PR of this node is not stable, add ReachCounter
                    context.getCounter(PageRank.ReachCounter.COUNT).increment(1);
                }
                PRNodeWritable newNode = new PRNodeWritable(key, new DoubleWritable(adjustedPR), node.getChildNum(), node.getAdjList());
                newNode.setXPR(new DoubleWritable(xPR));
                context.write(key, newNode);
            }
        }
    }
}
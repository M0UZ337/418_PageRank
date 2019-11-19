import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
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
        double m;
        long nodeCount;
        public void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            alpha = Double.parseDouble(conf.get("alpha"));
            m = 1.0 - Double.parseDouble(conf.get("m"));
            nodeCount = Long.parseLong(conf.get("nodeCount"));
        }
        public void reduce(IntWritable key, PRNodeWritable node, Context context) throws IOException, InterruptedException {
            double p = node.getPRValue().get();
            double xp = node.getXPR().get();
            double p2 = (alpha / nodeCount) + (1.0 - alpha) * ((m / nodeCount) + p);
            if(!(Math.abs(p - xp) < 0.00001)){
                // PR of this node is not stable, add ReachCounter
                context.getCounter(PageRank.ReachCounter.COUNT).increment(1);
            }
            PRNodeWritable newNode = new PRNodeWritable(key, new DoubleWritable(p2), node.getChildNum(), node.getAdjList());
            newNode.setXPR(new DoubleWritable(xp));
            context.write(key, node);
        }
    }
}
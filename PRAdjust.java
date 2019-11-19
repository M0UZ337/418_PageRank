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

    public static class PRAdjustMapper extends Mapper<IntWritable, PDNodeWritable, IntWritable, PDNodeWritable> {
        public void map(IntWritable key, PDNodeWritable node, Context context) throws IOException, InterruptedException {
            context.write(key, node);
        }

    }

    public static class PRAdjustReducer extends Reducer<IntWritable, PDNodeWritable, IntWritable, PDNodeWritable> { 
        double alpha;
        double m;
        long nodeCount;
        public void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            alpha = Double.parseDouble(conf.get("alpha"));
            m = (double) Long.parseLong(conf.get("m"));
            nodeCount = Long.parseLong(conf.get("nodeCount"));
        }
        public void reduce(IntWritable key, PDNodeWritable node, Context context) throws IOException, InterruptedException {
            m = context.getCounter( m ).getValue();
            double p = node.getPRValue().get();
            double p2 = (alpha / nodeNum) + (1.0 - alpha) * ((m / nodeNum) + p);
            node.setPRValue(new DoubleWritable(p2));
            context.write(key, node);
        }
    }
}
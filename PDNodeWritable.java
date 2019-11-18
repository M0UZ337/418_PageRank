import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.MapWritable;


public class PDNodeWritable implements Writable {
    
    public IntWritable nodeID;
    public IntWritable mass;
    public MapWritable adjList;
    
    public PDNodeWritable(IntWritable nodeID, IntWritable mass, MapWritable adjList) {
        this.nodeID = nodeID;
        this.mass = mass;
        this.adjList = adjList;
    }

    public void write(DataOutput out) throws IOException {
        nodeID.write(out);
        mass.write(out);
        adjList.write(out);
    }

    public void readFields(DataInput in) throws IOException {
        nodeID.readFields(in);
        mass.readFields(in);
        adjList.readFields(in);
    }
}
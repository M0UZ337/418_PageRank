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
    public BooleanWritable visited;
    public IntWritable distance;
    public MapWritable adjList;
    
    public PDNodeWritable(IntWritable nodeID, BooleanWritable visited, IntWritable distance, MapWritable adjList) {
        this.nodeID = nodeID;
        this.visited = visited;
        this.distance = distance;
        this.adjList = adjList;
    }

    public void write(DataOutput out) throws IOException {
        nodeID.write(out);
        visited.write(out);
        distance.write(out);
        adjList.write(out);
    }

    public void readFields(DataInput in) throws IOException {
        nodeID.readFields(in);
        visited.readFields(in);
        distance.readFields(in);
        adjList.readFields(in);
    }
}
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.MapWritable;


public class PDNodeWritable implements Writable {
    
    private IntWritable nodeID;
    private DoubleWritable prValue;
    private MapWritable adjList;
    
    public PDNodeWritable(IntWritable nodeID, DoubleWritable prValue, MapWritable adjList) {
        this.nodeID = nodeID;
        this.prValue = prValue;
        this.adjList = adjList;
    }

    public DoubleWritable getPRValue(){
        return prValue;
    }

    public void setPRValue(DoubleWritable prValue){
        this.prValue = prValue;
    }

    public MapWritable getAdjList(){
        return adjList;
    }

    public void write(DataOutput out) throws IOException {
        nodeID.write(out);
        prValue.write(out);
        adjList.write(out);
    }

    public void readFields(DataInput in) throws IOException {
        nodeID.readFields(in);
        prValue.readFields(in);
        adjList.readFields(in);
    }
}
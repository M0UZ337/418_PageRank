import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.MapWritable;


public class PRNodeWritable implements Writable {
    
    private IntWritable nodeID;
    private DoubleWritable prValue;
    private DoubleWritable xPR;
    private IntWritable childNum;
    private MapWritable adjList;
    
    // default constructor
    public PRNodeWritable(){
        this.nodeID = new IntWritable();
        this.prValue = new DoubleWritable();
        this.xPR = new DoubleWritable();
        this.childNum = new IntWritable();
        this.adjList = new MapWritable();
    }
    
    public PRNodeWritable(IntWritable nodeID, DoubleWritable prValue, IntWritable childNum, MapWritable adjList){
        this.nodeID = nodeID;
        this.prValue = prValue;
        this.xPR = new DoubleWritable(-1.0);
        this.childNum = childNum;
        this.adjList = adjList;
    }

    public IntWritable getNodeID(){
        return nodeID;
    }

    public DoubleWritable getPRValue(){
        return prValue;
    }

    public void setPRValue(DoubleWritable newPRValue){
        this.prValue = newPRValue;
    }

    public DoubleWritable getXPR(){
        return xPR;
    }

    public void setXPR(DoubleWritable newXPR){
        this.prValue = newXPR;
    }

    public IntWritable getChildNum(){
        return childNum;
    }

    public MapWritable getAdjList(){
        return adjList;
    }

    public Text toText(){
        String output = "";
        output = output + "nodeID: " + this.nodeID.toString();
        output = output + " prValue: " + this.prValue.toString();
        output = output + " XPR: " + this.xPR.toString();
        output = output + " childNum: " + this.childNum.toString();
        output = output + " adjList: ";
        for(Map.Entry<Writable, Writable> node : this.adjList.entrySet()){
            output = output + "(" + node.getKey().toString() + "," + node.getValue().toString() + ") ";
        }
        return new Text(output);
    }

    public void write(DataOutput out) throws IOException {
        nodeID.write(out);
        prValue.write(out);
        xPR.write(out);
        childNum.write(out);
        adjList.write(out);
    }

    public void readFields(DataInput in) throws IOException {
        nodeID.readFields(in);
        prValue.readFields(in);
        xPR.readFields(in);
        childNum.readFields(in);
        adjList.readFields(in);
    }
}
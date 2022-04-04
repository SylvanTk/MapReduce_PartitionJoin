package hadoop.mapreduce.patterns.PartitionJoin;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class PairWritable implements Writable {
    public String Tag;
    public Writable Object;

    public PairWritable(){

    }

    public void inputWritable(Writable writable){
        Object =  writable instanceof Human ? (Human) writable : (Job) writable;
    }

    public void inputTag(String tag){
        Tag = tag;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(Tag);
        Object.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        Tag = dataInput.readUTF();
        Object = Tag.equals("human") ? new Human() : new Job();
        Object.readFields(dataInput);
    }
}

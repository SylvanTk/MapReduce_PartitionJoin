package hadoop.mapreduce.patterns.PartitionJoin;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

public class Job implements Writable {
    public int id;
    public String JobName;

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(id);
        dataOutput.writeUTF(JobName);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        id = dataInput.readInt();
        JobName = dataInput.readUTF();
    }

    public void insertInfo(ArrayList<String> data){
        id = Integer.parseInt(data.get(0));
        JobName = data.get(1);
    }

    @Override
    public String toString() {
        return String.format("%d %s \t", id, JobName);
    }
}

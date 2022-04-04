package hadoop.mapreduce.patterns.PartitionJoin;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

public class Human implements Writable {
    public int id;
    public String Name;
    public String LastName;
    public int Age;
    public String Sex;
    public int job_id;

    public Human(){

    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(id);
        dataOutput.writeUTF(Name);
        dataOutput.writeUTF(LastName);
        dataOutput.writeInt(Age);
        dataOutput.writeUTF(Sex);
        dataOutput.writeInt(job_id);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        id = dataInput.readInt();
        Name = dataInput.readUTF();
        LastName = dataInput.readUTF();
        Age = dataInput.readInt();
        Sex = dataInput.readUTF();
        job_id = dataInput.readInt();
    }

    public void insertInfo(ArrayList<String> data){
        id = Integer.parseInt(data.get(0));
        Name = data.get(1);
        LastName = data.get(2);
        Age = Integer.parseInt(data.get(3));
        Sex = data.get(4);
        job_id = Integer.parseInt(data.get(5));
    }

    @Override
    public String toString() {
        return String.format("%d %s %s %d %s %d \t", id, Name, LastName, Age, Sex, job_id);
    }
}

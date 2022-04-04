package hadoop.mapreduce.patterns.PartitionJoin;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.StringTokenizer;

public class PartitionJoin {

    public static class PeopleMapper extends Mapper<Object, Text, IntWritable, PairWritable>{

        @Override
        protected void map(Object key, Text value, Mapper<Object, Text, IntWritable, PairWritable>.Context context) throws IOException, InterruptedException {
            StringTokenizer stringTokenizer = new StringTokenizer(value.toString());
            ArrayList<String> humanSplitted = new ArrayList<>();
            while(stringTokenizer.hasMoreTokens())
                humanSplitted.add(stringTokenizer.nextToken());

            Human human = new Human();
            human.insertInfo(humanSplitted);

            PairWritable humanWritable = new PairWritable();
            humanWritable.inputTag("human");
            humanWritable.inputWritable(human);

            context.write(new IntWritable(human.job_id), humanWritable);
        }

    }

    public static class JobsMapper extends Mapper<Object, Text, IntWritable, PairWritable>{
        @Override
        protected void map(Object key, Text value, Mapper<Object, Text, IntWritable, PairWritable>.Context context) throws IOException, InterruptedException {
            StringTokenizer stringTokenizer = new StringTokenizer(value.toString());
            ArrayList<String> jobSplitted = new ArrayList<>() ;
            while(stringTokenizer.hasMoreTokens())
                jobSplitted.add(stringTokenizer.nextToken());

            Job job = new Job();
            job.insertInfo(jobSplitted);

            PairWritable jobPair = new PairWritable();
            jobPair.inputTag("job");
            jobPair.inputWritable(job);

            context.write(new IntWritable(job.id), jobPair);
        }
    }

    public static class PartitionReducer extends Reducer<IntWritable, PairWritable, IntWritable, Text>{
        @Override
        protected void reduce(IntWritable key, Iterable<PairWritable> values, Reducer<IntWritable, PairWritable, IntWritable, Text>.Context context) throws IOException, InterruptedException {
            HashMap<String, ArrayList<Writable>> hashMap = new HashMap<>();

            for(PairWritable pair : values){
                hashMap.merge(pair.Tag, new ArrayList<Writable>() { { add(pair.Object); } }, (oldValue, newValue) -> new ArrayList<Writable>() { { addAll(oldValue); addAll(newValue); } });
            }


            for(Writable human : hashMap.get("human"))
                for(Writable job : hashMap.get("job"))
                    context.write(key, new Text( human.toString() + job.toString()));
        }
    }

}

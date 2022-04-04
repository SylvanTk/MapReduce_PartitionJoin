package hadoop.mapreduce.patterns;

import hadoop.mapreduce.patterns.MapReduceJobRunner.MapReduceJobRunner;

public class Main {

    public static void main(String[] args) throws Exception {
        MapReduceJobRunner.RunPartitionJoinJob(args);
    }
}

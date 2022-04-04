package hadoop.mapreduce.patterns.MapReduceJobRunner;

import hadoop.mapreduce.patterns.PartitionJoin.PartitionJoinJob;
import org.apache.hadoop.util.ToolRunner;

public class MapReduceJobRunner {

    public MapReduceJobRunner(){

    }

    public static int RunPartitionJoinJob(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new PartitionJoinJob(), args);

        return exitCode;
    }

}

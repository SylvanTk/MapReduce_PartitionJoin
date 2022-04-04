package hadoop.mapreduce.patterns.PartitionJoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

public class PartitionJoinJob extends Configured implements Tool {
    private static final String jobName = "CrossCorrelationPairs";
    private static final String inputPathPeople = "hdfs://localhost:9000/user/petya/mydata/PartitionJoin/db/people";
    private static final String inputPathJobs = "hdfs://localhost:9000/user/petya/mydata/PartitionJoin/db/jobs";
    private static final String outputPath = "hdfs://localhost:9000/user/petya/mydata/PartitionJoin/output";

    private static final String coreSitePath = "/home/petya/Work/Hadoop/hadoop-2.10.1/etc/hadoop/core-site.xml";
    private static final String hdfsSitePath = "/home/petya/Work/Hadoop/hadoop-2.10.1/etc/hadoop/hdfs-site.xml";

    @Override
    public int run(String[] strings) throws Exception {
        // Объявляем job
        Configuration configuration = getConf();
        configuration.addResource(new Path(coreSitePath));
        configuration.addResource(new Path(hdfsSitePath));

        Job partitionJoinJob = Job.getInstance(configuration, jobName);
        partitionJoinJob.setJarByClass(getClass());


        // Назначаем путь к входным данным (файлам)
        // Используем множественный ввод данных из разных файлов
        MultipleInputs.addInputPath(partitionJoinJob, new Path(inputPathPeople), TextInputFormat.class, PartitionJoin.PeopleMapper.class);
        MultipleInputs.addInputPath(partitionJoinJob, new Path(inputPathJobs), TextInputFormat.class, PartitionJoin.JobsMapper.class);

        // Назначаем путь к выходным данным
        FileOutputFormat.setOutputPath(partitionJoinJob, new Path(outputPath));

        partitionJoinJob.setMapOutputKeyClass(IntWritable.class);
        partitionJoinJob.setMapOutputValueClass(PairWritable.class);
        partitionJoinJob.setOutputKeyClass(IntWritable.class);
        partitionJoinJob.setOutputValueClass(Text.class);

        partitionJoinJob.setReducerClass(PartitionJoin.PartitionReducer.class);

        return partitionJoinJob.waitForCompletion(true) ? 0 : 1;
    }
}

package p1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by zehafta on 11/27/17.
 */
public class MyDriver {
    public static void main(String[] args) throws IOException,InterruptedException,ClassNotFoundException{
        Path input_dir = new Path("hdfs://localhost:54310/user/hduser/youtube");
        Path output_dir = new Path("hdfs://localhost:54310/user/hduser/toptenViews");
        Configuration conf = new Configuration();
        Job j =  Job.getInstance(conf,"YouTubeTop10Views");
        j.setJarByClass(MyDriver.class);
        j.setNumReduceTasks(1);
        j.setMapperClass(MyMapper.class);
        j.setMapOutputKeyClass(Text.class);
        j.setMapOutputValueClass(Text.class);
        j.setReducerClass(MyReducer.class);

        FileInputFormat.addInputPath(j,input_dir);
        FileOutputFormat.setOutputPath(j,output_dir);
        output_dir.getFileSystem(j.getConfiguration()).delete(output_dir,true);
        j.waitForCompletion(true);

    }
}

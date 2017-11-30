package p1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by zehafta on 11/27/17.
 */
public class MyMapper extends Mapper<LongWritable, Text,Text,Text>{
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
         String data[] = value.toString().split("\t");
    if(data.length<6) return;
    String views = data[5];
    String video_id=data[0];
    context.write(new Text("KEY"),new Text(video_id+":"+views));

    }
}

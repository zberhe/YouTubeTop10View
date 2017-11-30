package p1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * Created by zehafta on 11/27/17.
 */
public class MyReducer extends Reducer<Text,Text,Text,IntWritable> {
    private List<YouTube> videoList = new ArrayList<>();
    public void reduce(Text key, Iterable<Text>values,Context context) throws IOException,InterruptedException{
       // String data[] = value.toString().split(":");

        //int ratingfromMapper = Integer.parseInt(data[1]);
      //  System.out.println("FROM: "+data[0]);
        YouTube youTube;
 for(Text value: values){
     String data[] = value.toString().split(":");
     String videoID = data[0];
     int videoViews = Integer.parseInt(data[1]);
      youTube = new YouTube(videoID.toString(),videoViews);
     videoList.add(youTube);
 }
    }

    class YouTube implements Comparable<YouTube>{
        public String video_id =null;
        public int totalViews;
        public YouTube(String video_id,int totalViews){
            this.video_id = video_id;
            this.totalViews = totalViews;
        }


        @Override
        public int compareTo(YouTube youTube) {
            return -(totalViews - youTube.totalViews);
        }
    }
    @Override
    public void cleanup(Context context) throws IOException,InterruptedException{
        Collections.sort(videoList);
        for (int i=0;i<10;i++){
          YouTube youTube =videoList.get(i);
          context.write(new Text(youTube.video_id),new IntWritable(youTube.totalViews));
        }

    }

}


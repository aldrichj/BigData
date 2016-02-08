/**
 * Created by Jeremy on 1/31/2016.
 */

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class NGramReducer   extends Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text key, Iterable<Text> values,
                       Context context)
            throws IOException, InterruptedException {



        Map titles = new HashMap<>();
        int sum = 0;
        int bookSum = 0;
        String total = "0";
        String title = null;



        for (Text value : values) {
            String[] items = value.toString().split(",");
            sum += Integer.parseInt(items[0]);

            // Checks if title is already in map
            // if the title is in the map then we know its a new volume.
            title = getTitle(items);
            if(!titles.containsKey(title)){
                bookSum++;
                titles.put(title,1);
            }


        }
        total = Integer.toString(sum);

        titles.clear();

        context.write(key, new Text(total+" \t "+bookSum));


    }


    private String getTitle(String[] items){

        String title = items[2];

        title = title.replace("$","");


        return title;
    }

}

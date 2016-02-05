/**
 * Created by Jeremy on 1/31/2016.
 */

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class NGramReducer   extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context)
            throws IOException, InterruptedException {



       //System.out.println("----------------------------------------- Reducer---------------------------------------------------------------");
        Map map = new HashMap<>();
        String tokens[] = null;
        int sum = 0;
        int sumBooks = 0;
        for (IntWritable value : values) {
            sum += value.get();
            tokens = key.toString().split("\\s+");

            String item = map.get(tokens[0]).toString();

            if(item == null){
                map.put(tokens[0],1);

            }else{

            }





            System.out.println(key);
        }

        context.write(key, new IntWritable(sum));


    }



}

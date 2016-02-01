/**
 * Created by Jeremy on 1/31/2016.
 */

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class NGramMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private static final int MISSING = 9999;
    private static  String delim = "[ ]+";
    private String author = null;
    private int year;
    private boolean headerFlag = false;


    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString();
        String[] tokens = line.split(delim);





        if (tokens[0].equals("Author:")) {
            author = tokens[tokens.length - 1];

        } else if (tokens[0].equals("Release") && tokens[1].equals("Date:")) {
            year = Integer.parseInt(tokens[4]);

        }


        // Header finished start the real scan
        if (tokens[tokens.length - 1].equals("***")) {
            headerFlag = true;

        }

        if(headerFlag){

            ArrayList<String> ngrams = nGrams(1,line);
            for (String word : ngrams) {
                context.write(new Text(word), new IntWritable(year));
            }


        }



    }

    private ArrayList<String> nGrams(int n, String line){
        String[] tok = line.split("\\s+");
        ArrayList<String> ngrams = new ArrayList<String>();

        for(int i=0; i<(tok.length-n+1); i++) {
            String s = "";
            int start = i;
            int end = i + n;

            for (int j = start; j < end; j++) {
                s = s + "" + tok[j];
            }
            //Add n-gram to a list
            ngrams.add(s);
        }


        return ngrams;
    }

}

/**
 * Created by Jeremy on 1/31/2016.
 */

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;

public class NGramMapper  extends Mapper<NullWritable, BytesWritable, Text, IntWritable> {


    private static  String delim = "[ ]+";
    private String author = null;
    private String year;
    private boolean headerFlag = false;


    @Override
    public void map(NullWritable key, BytesWritable value, Context context)
            throws IOException, InterruptedException {
        ArrayList<String> ngrams;
        byte[] content = value.getBytes();


        Configuration conf = context.getConfiguration();
        String param = conf.get("N");
        String sortopt = conf.get("sortby");
        String sortby = "yer";
        int N = Integer.parseInt(param);
        int size = content.length;
        InputStream is;
        byte[] b = new byte[size];


        if(sortopt.equals("y"))
            sortby = "year";
        else if(sortopt.equals("a"))
            sortby = "author";




        try {
            is = new ByteArrayInputStream(content);
            is.read(b);
            System.out.println("Data Recovered: "+new String(b));
            String tex = new String(b);
            String[] li = tex.split("\\r?\\n");

            System.out.println("**********");

            for(int i = 0;i<li.length-1;i++){

                if(!li[i].isEmpty())
                    System.out.println(li[i]);
            }


            System.out.println("----------");

            for(int i = 0;i<li.length-1;i++){


                if(!li[i].isEmpty()) {
                    if (!headerFlag) {

                        String[] tokens = li[i].toString().split(delim);


                        if (tokens[0].equals("Author:")) {
                            author = tokens[tokens.length - 1];
                            System.out.println("Author: "+author);


                        } else if (tokens[0].equals("Released") && tokens[1].equals("Date:")) {
                            year = tokens[4];
                            System.out.println("Year: "+year);


                        }

                        //Header finished start the real scan
                        if (tokens[tokens.length - 1].equals("***")) {
                            headerFlag = true;
                            System.out.println("Header End:***");

                        }


                    } else {

                        ngrams = nGrams(N, li[i]);

                        for (String tok : ngrams) {



                            System.out.println(tok);
                            context.write(new Text(tok.toString()+"    "+sortby), new IntWritable(1));

                        }
                    }
                }
            }


            is.close();


        } catch (IOException e) {
            e.printStackTrace();
        } finally {

        }


    }
    //TODO: setup n-gram where the first index is blank. If n >
    private ArrayList<String> nGrams(int n, String line){
        String[] tok = line.split("\\s+");
        ArrayList<String> ngrams = new ArrayList<>();
        boolean first = true;

        for(int i=0; i<(tok.length-n+1); i++) {
            String s = "";
            int start = i;
            int end = i + n;
            first = true;

            for (int j = start; j < end; j++) {

                s = s.replaceAll("^\\s+", "");
                s = s + " " + tok[j];


            }


            /* Add n-gram to a list */


            ngrams.add(s.toLowerCase());
        }

        if(tok.length % n != 0){
            ngrams.add(tok[tok.length-1].toLowerCase()+" ");
        }



        return ngrams;
    }

}

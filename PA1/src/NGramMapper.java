/**
 * Created by Jeremy on 1/31/2016.
 */

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.text.BreakIterator;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
           // System.out.println("Data Recovered: "+new String(b));
            String tex = new String(b);
            Pattern endHeader = matcherEndHeader();
            Pattern endBook = matcherEndStory();

            Matcher endHead = endHeader.matcher(tex);
            Matcher endStory = endBook.matcher(tex);



            String[] li = tex.split("\\r?\\n");
            BreakIterator bi = BreakIterator.getSentenceInstance();
            bi.setText(tex.replaceAll("(\\t|\\r?\\n)+", " "));



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
                            System.out.println("Header End:***"+author+" "+year);
                            break;
                        }

                    }
                }
            }


//            int index = 0;
//            while (bi.next() != BreakIterator.DONE) {
//                String sentence = tex.substring(index, bi.current());
//                System.out.println("Sentence: " + sentence);
//                index = bi.current();
//                ngrams = nGrams(N, sentence);
//                for (String tok : ngrams) {
//
//
//
//                    // System.out.println(tok);
//                    context.write(new Text(tok.toString()+"    "+sortby), new IntWritable(1));
//
//                }
//            }






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
                // replaces all non letter characters including left white spaces
                s = s.replaceAll("[^a-zA-Z\\s]", "").replaceAll("^\\s+", "");
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

    private Pattern matcherEndHeader(){
        String re1="(\\*)";	// Any Single Character 1
        String re2="(\\*)";	// Any Single Character 2
        String re3="(\\*)";	// Any Single Character 3
        String re4=".*?";	// Non-greedy match on filler
        String re5="(START)";	// Word 1
        String re6=".*?";	// Non-greedy match on filler
        String re7="((?:[a-z][a-z]+))";	// Word 2
        String re8=".*?";	// Non-greedy match on filler
        String re9="(\\*)";	// Any Single Character 4
        String re10="(\\*)";	// Any Single Character 5
        String re11="(\\*)";	// Any Single Character 6

        return Pattern.compile(re1+re2+re3+re4+re5+re6+re7+re8+re9+re10+re11,Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

    }

    private Pattern matcherEndStory(){
        String re1="(\\*)";	// Any Single Character 1
        String re2="(\\*)";	// Any Single Character 2
        String re3="(\\*)";	// Any Single Character 3
        String re4=".*?";	// Non-greedy match on filler
        String re5="(END)";	// Word 1
        String re6=".*?";	// Non-greedy match on filler
        String re7="((?:[a-z][a-z]+))";	// Word 2
        String re8=".*?";	// Non-greedy match on filler
        String re9="(\\*)";	// Any Single Character 4
        String re10="(\\*)";	// Any Single Character 5
        String re11="(\\*)";	// Any Single Character 6

        return Pattern.compile(re1+re2+re3+re4+re5+re6+re7+re8+re9+re10+re11,Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

    }

}

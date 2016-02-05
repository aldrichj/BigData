/**
 * Created by Jeremy on 1/31/2016.
 */

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
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







        try {

            is = new ByteArrayInputStream(content);
            DataInputStream din = new DataInputStream(is);
            din.readFully(b);
           // System.out.println("Data Recovered: "+new String(b));
            String tex = new String(b);
            Pattern endHeader = patternEndHeader();
            Pattern endBook = patternEndStory();
            Pattern sent = patternSentence();
            String[] metaInfo = null;
            Matcher reMatcher = sent.matcher(tex);
            while (reMatcher.find()) {
                Matcher m = endHeader.matcher(reMatcher.group());
                Matcher n = endBook.matcher(reMatcher.group());

                if(!n.find()) {
                    // Header information
                    if (!headerFlag && m.find()) {
                        metaInfo = findHeaderInfo(reMatcher.group());
                        headerFlag = true;
                        author = metaInfo[0];
                        year = metaInfo[1];

                        if (sortopt.equals("y"))
                            sortby = year;
                        else if (sortopt.equals("a"))
                            sortby = author;

                        //System.out.println(metaInfo[0]+" "+metaInfo[1]);
                    }

                    //Story until we find the end
                    else if (headerFlag && !n.find()) {

                        ngrams = nGrams(N, reMatcher.group().toString().replaceAll("\\p{Punct}", " "));

                        for (String ng : ngrams) {
                            if (!ng.isEmpty()) {

                                context.write(new Text(ng.toString() + "\t" + sortby), new IntWritable(1));
                            }

                        }
                    }
                }else{
                    break;
                }
            }


            is.close();
            din.close();


        } catch (IOException e) {
            e.printStackTrace();
        } finally {

        }


    }
    //TODO: setup n-gram where the first index is blank. If n >
    private ArrayList<String> nGrams(int n, String line){
        if(n > 1) {
            line = "_Start_ " + line + " _End_";
        }

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

                s = s + " " + tok[j];


            }


            /* Add n-gram to a list */
            ngrams.add(s.toLowerCase());
        }


//        if(tok.length % n != 0){
//            ngrams.add(tok[tok.length-1].toLowerCase()+" ");
//        }



        return ngrams;
    }


    private String findYear(String text){


        Pattern p = Pattern.compile("\\d{4}");  // insert your pattern here
        Matcher m = p.matcher(text);
        int position = 0;
        if (m.find()) {
            position = m.start();
        }

        return m.group().toString();

    }

    private String[] findHeaderInfo(String text){
        String author = null;
        String year = null;
        boolean headerFlag = false;
        String[] tokens = text.split("\\r?\\n");
        for(int i = 0;i<tokens.length-1;i++){

            if(!tokens[i].isEmpty()) {

                if (tokens[i].contains("Author:")) {
                    String[] tok = tokens[i].split("[ ]");
                    author = tok[tok.length - 1];
                    System.out.println("Author: "+author);


                } else if (tokens[i].contains("Release Date")) {
                    year = findYear(tokens[i]);
                    System.out.println("Year: "+year);

                }
            }
        }

        return new String[]{author, year};

    }

    private Pattern patternEndHeader(){
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

    private Pattern patternEndStory(){
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


    private Pattern patternSentence(){
        Pattern re = Pattern.compile(
                "# Match a sentence ending in punctuation or EOS.\n" +
                        "[^.!?\\s]    # First char is non-punct, non-ws\n" +
                        "[^.!?]*      # Greedily consume up to punctuation.\n" +
                        "(?:          # Group for unrolling the loop.\n" +
                        "  [.!?]      # (special) inner punctuation ok if\n" +
                        "  (?!['\"]?\\s|$)  # not followed by ws or EOS.\n" +
                        "  [^.!?]*    # Greedily consume up to punctuation.\n" +
                        ")*           # Zero or more (special normal*)\n" +
                        "[.!?]?       # Optional ending punctuation.\n" +
                        "['\"]?       # Optional closing quote.\n" +
                        "(?=\\s|$)",
                Pattern.MULTILINE | Pattern.COMMENTS);

        return re;

    }

}

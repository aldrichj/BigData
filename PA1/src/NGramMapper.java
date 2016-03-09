/**
 * Parses through chunks sent in by HDFS. Each chunk should be 64MB or less.
 * Each book should have the generic Project Gutenberg header.
 *
 * The basic structure of the program is we parse through the header first.
 * Then parse through the remaining text sentence by sentence. During the parsing we are
 * generating N-grams, and passing this information into HDFS to be fed to the reducer.
 *
 * Created by Jeremy on 1/31/2016.
 */

import java.io.*;
import java.text.BreakIterator;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;



public class NGramMapper  extends Mapper<NullWritable, BytesWritable, Text, Text> {


    private static  String delim = "[ ]+";
    private String author = null;
    private String year;
    private String title;


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


        /**
         * This section of code parses the file including finding the header meta info
         *
         */


        try {

            is = new ByteArrayInputStream(content);
            DataInputStream din = new DataInputStream(is);
            din.readFully(b);

            String tex = new String(b);
            Pattern endHeader = patternEndHeader();
            Pattern endBook = patternEndStory();
            Pattern sent = patternSentence();
            String[] metaInfo = null;


            metaInfo = findHeaderInfo(tex);

            author = metaInfo[0];
            year = metaInfo[1];
            title = metaInfo[2];

            // sets sortby based on users intial input
            if (sortopt.equals("y"))
                sortby = year;
            else if (sortopt.equals("a"))
                sortby = author;

            // removes the header section of the text
            // so we only parse the book section
            tex = tex.substring(tex.indexOf("***"));
            tex = tex.substring(tex.indexOf("***"));
            Matcher reMatcher = sent.matcher(tex);

            // keeps going until end of file or no more sentences
            while (reMatcher.find()) {
                //Matcher m = endHeader.matcher(reMatcher.group());
                Matcher n = endBook.matcher(reMatcher.group());




                    //Story until we find the end
                    if (!n.find()) {
                        // generates ngrams and returns an arraylist of them per sentence
                        // and also removes all punctuation
                        String sentence = reMatcher.group().toString().replaceAll(","," ");
                        ngrams = nGrams(N, sentence.replaceAll("\\p{Punct}", ""));


                        for (String ng : ngrams) {
                            if (!ng.isEmpty()) {
                                /**
                                 * writes it out to a file to be shuffled and partitioned by HDFS
                                 * then passed to the reducer.
                                 * The arguments are word, <author | year>, in book count, unique volumes>
                                 */
                                context.write(new Text(ng.toString() + "\t" + sortby), new Text("1, 1, "+title));
                            }

                        }
                    }

            }


            is.close();
            din.close();


        } catch (IOException e) {
            e.printStackTrace();
        } finally {

        }


    }

    /**
     * Generates the ngram requested by the user
     * based code from: http://www.text-analytics101.com/2014/11/what-are-n-grams.html
     * I modified it to fit my needs
     * @param n - the value of gram we want
     * @param line the sentence
     * @return arrayList of ngrams
     */
    private ArrayList<String> nGrams(int n, String line){
        if(n > 1) {
            line = "_Start_ " + line + " _End_";
        }
        line = line.replaceAll("\\n?\\r","");
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



        return ngrams;
    }

    /**
     * Looks for the title in the header and returns it.
     * @param text
     * @return title of the book
     */
    private String findTitle(String text){
        String re1="(Title)";	// Word 1
        String re2="(:)";	// Any Single Character 1

        Pattern p = Pattern.compile(re1+re2,Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
        Matcher m = p.matcher(text);
        if(m.find()){
            return text.substring(m.end()).trim();
        }
        return "Nan";
    }

    /**
     * Looks for the author
     * @param text
     * @return returns the full name
     */
    private String findAuthor(String text){


        String re1="(Author)";	// Word 1
        String re2="(:)";	// Any Single Character 1

        Pattern p = Pattern.compile(re1+re2,Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
        Matcher m = p.matcher(text);
        if(m.find()){
            return text.substring(m.end()).trim().replaceAll("[^A-Za-z ]", "");
        }
        return "Nan";

    }

    /**
     *  Finds and returns the year in YYYY format
     * @param text
     * @return the year
     */
    private String findYear(String text){


        Pattern p = Pattern.compile("\\d{4}");  // insert your pattern here
        Matcher m = p.matcher(text);
        int position = 0;
        // needed this or I got an exception.
        if (m.find()) {
            position = m.start();
        }

        return m.group().toString();

    }

    /**
     *  Finds and returns the title, author, release date of book
     * @param text
     * @return
     */
    private String[] findHeaderInfo(String text){
        String author = "author";
        String year = null;
        String title = "author";
        boolean headerFlag = false;
        String[] tokens = text.split("\\r?\\n");
        for(int i = 0;i<tokens.length-1;i++){

            if(!tokens[i].isEmpty()) {




                if (tokens[i].contains("Title")) {
                    //System.out.println("Title Found!");
                    title = findTitle(tokens[i].toString());


                } else if (tokens[i].contains("Author:")) {
                    //System.out.println("Author Found!");
                    String auth[] = findAuthor(tokens[i].toString()).split("\\s+");

                    author = auth[auth.length-1];


                } else if (tokens[i].contains("Release Date")) {
                    //System.out.println("Year Found!");
                    year = findYear(tokens[i].toString());
                    //System.out.println("Year: "+year);
                   // System.out.println(author+" "+year+" "+title);
                    return new String[]{author, year, title};
                }


            }
        }



        return new String[]{author, year, title};

    }

    /**
     * End of header pattern
     * regex generated by: http://txt2re.com/
     * @return pattern
     */
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

    /**
     * End of story pattern
     * regex generated by: http://txt2re.com/
     * @return pattern
     */
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

    /**
     * Sentence pattern
     * Found regex on stackoverflow
     * @return pattern
     */
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
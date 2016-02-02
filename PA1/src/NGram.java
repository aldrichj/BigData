import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Created by Jeremy on 1/31/2016.
 */
public class NGram {


    public static void main(String[] args) throws Exception {

        if (args.length != 3) {
            System.out.printf("Usage: ProcessLogs <input dir> <output dir> <number of grams>: Default 1\n");
            System.exit(-1);
        }

        int number = 1;
        if(args.length == 3) {
            number = testInt(args[2]);

            if(number == -1){
                System.out.println("N-Grams must be greater than 0");
            }
        }



        Configuration conf = new Configuration();
        conf.set("N", args[2]);
        Job job = new Job(conf);

        job.setJarByClass(NGram.class);
        job.setJobName("NGram");
        job.setInputFormatClass(WholeFileInputFormat.class);

        //FileInputFormat.setInputPaths(job, new Path(args[0]));
        WholeFileInputFormat.setInputPaths(job, new Path(args[0]));

        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(NGramMapper.class);
        job.setReducerClass(NGramReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        boolean success = job.waitForCompletion(true);
        System.exit(success ? 0 : 1);
    }



    private static int testInt(String num){

        int number;
        try {
            number = Integer.parseInt(num);
        }catch(NumberFormatException  e){
            return -1;
        }

        if(number <= 0 )
            return -1;


        return number;
    }

}



import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Created by Jeremy on 1/31/2016.
 */
public class NGram {


    public static void main(String[] args) throws Exception {

        if (args.length != 2) {
            System.err.println("Usage: NGrame <input path> <output path>");
            System.exit(-1);
        }

        Job job = new Job();
        job.setJarByClass(NGram.class);
        job.setJobName("N-Gram");

//        Configuration conf = getConf();
//        conf.set("N.property", args[2]);


        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(NGramMapper.class);
        job.setReducerClass(NGramReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }

}






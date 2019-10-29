import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.StringTokenizer;

import static java.lang.String.*;

//import org.apache.hadoop.mapreduce.util.GenericOptionsParser;


//  Word Count for Part 2 (WordGrep no zip)

//public class WordCount
//{
//        public static class TokenizerMapper extends
//                Mapper<Object, Text, Text, IntWritable> {
//            private int count = 0;
//
//            public void map(Object key, Text value, Context context)
//                    throws IOException, InterruptedException {
//                String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
//
//                String temp = "";
//                if (value.toString().toLowerCase().contains("door")){
//                    if(value.toString().length() > 50) {
//                        temp = (fileName + " " + key + " " + value.toString().substring(0, 50));
//                    }
//                    else {
//                        temp = (fileName + " " + key + " " + value);
//                    }
//
//                    context.write(new Text(temp),new IntWritable(count));
//
//                }
//                count++;
//
//            }
//        }
//
//        public static class IntSumReducer extends
//                Reducer<Text, IntWritable, Text, IntWritable> {
//            private IntWritable c = new IntWritable(0);
//            HashMap<String, Integer>input = new HashMap<String, Integer>();
//            public void reduce(Text key, Iterable<IntWritable> values,
//                               Context context) throws IOException, InterruptedException {
//
//                for(IntWritable val  : values){
//                    c = val;
//                }
//
//                context.write(key, c);
//
//            }
//        }
//
//
//        public static void main(String[] args) throws Exception {
//        Configuration conf = new Configuration();
//        Job job = Job.getInstance(conf, "word count");
//        job.setJarByClass(WordCount.class);
//        job.setInputFormatClass(TextInputFormat.class);
//        job.setMapperClass(WordCount.TokenizerMapper.class);
//        job.setCombinerClass(IntSumReducer.class);
//        job.setReducerClass(IntSumReducer.class);
//
//        job.setOutputKeyClass(Text.class);
//        job.setOutputValueClass(IntWritable.class);
//        FileInputFormat.addInputPath(job, new Path(args[0]));
//        FileOutputFormat.setOutputPath(job, new Path(args[1]));
//        System.exit(job.waitForCompletion(true) ? 0 : 1);
//    }
//
//}



// Word Count for Part 3(Wordgrep with zip file)


public class WordCount
{
        public static class TokenizerMapper extends
                Mapper<Text, BytesWritable, Text, IntWritable> {

            private int count = 0;
            private final static IntWritable one = new IntWritable(1);
            private Text word = new Text();
            int counted = 0;


            public void map(Text key, BytesWritable value, Context context)
                    throws IOException, InterruptedException {
                String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
                String vstring = new String(value.getBytes());


                //byte[] byteArr = value.getBytes();
                //String v = Arrays.toString(byteArr);
               // String linebreak = "[.-:#,/ ]+";
                //
//                String vstring2 = vstring.replace(Character.MIN_VALUE, '\n');
//                vstring2 = vstring2.replace("\n", " \n ");


                String temp = "";
                int line_no = 0;
                int offset = 0;

                StringTokenizer itr = new StringTokenizer(vstring, "\n");

                //Iterate over the value-string line by line
                while(itr.hasMoreTokens()){
                    //Set current token
                    String current_line  = itr.nextToken();
                        if (current_line.toLowerCase().contains("door")) {

                            String line_content = "";
                            if(current_line.length() >= 50){
                                current_line = current_line.substring(0,50);
                            }
                            else{
                                //maintain line length
                            }
                            temp = new String(key + " " + line_no + " " + current_line);
                            context.write(new Text(temp), new IntWritable(offset));

                        }
                    //Increment the line number
                    line_no++;
                    //Increment line offset
                    offset+= current_line.length();
                }
            }


        }

        public static class IntSumReducer extends
                Reducer<Text, IntWritable, Text, IntWritable> {
            private IntWritable c = new IntWritable(0);
            public void reduce(Text key, Iterable<IntWritable> values,
                               Context context) throws IOException, InterruptedException {

                for(IntWritable val  : values){
                    c = val;
                }
                context.write(key, c);

            }
        }


        public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(WordCount.class);
            job.setInputFormatClass(NYUZInputFormat.class);
            job.setMapperClass(WordCount.TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}







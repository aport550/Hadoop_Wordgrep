//package edu.nyu.tandon.bigdata.hadoop;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import java.util.zip.ZipInputStream;
/**
 * Extends the basic FileInputFormat to accept ZIP files.
 * ZIP files are not 'splittable', so we need to process/decompress in place:
 * each ZIP file will be processed by a single Mapper; we are parallelizing files, not lines...
 */
public class NYUZInputFormat extends FileInputFormat<Text, BytesWritable> {
//public class NYUZInputFormat extends TextInputFormat {

    //    @Override
//    public List<InputSplit> getSplits(JobContext context)
//            throws IOException, InterruptedException {
//
//        // your code here
//        // our splits will consist of whole files found insize our input zip file
//        // return splits....
//
//        // The goal here is to not split the zipped file into chunks so we can accept the whole file at once
//        // Can't partition into chunks because zipped files begin with metadata and cant be split
//        protected boolean isSplitable(JobContext context, Path path){
//            return false;
//        }
//
//
//
//
//    }
    protected boolean isSplitable(JobContext context, Path path) {
        return false;
    }

    /*** return a record reader
     *
     * @param split
     * @param context
     * @return (Text, BytesWritable)
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
//    public RecordReader<Text, BytesWritable> createRecordReader(InputSplit split, TaskAttemptContext context)
    public RecordReader<Text, BytesWritable> createRecordReader(InputSplit split, TaskAttemptContext context)
            throws IOException, InterruptedException {
        {
            // no need to modify this one....
            return new NYUZRecordReader();
        }
    }
}
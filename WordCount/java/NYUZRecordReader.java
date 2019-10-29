//package edu.nyu.tandon.bigdata.hadoop;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.StringTokenizer;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.LineReader;

/*** Custom Hadoop Record Reader : zipped file
 *
 * We want to produce (K,V) pairs where
 *    K = filename inside the zip file
 *    V = bytes corresponding to the file
 *
 * ***/
 public class NYUZRecordReader extends RecordReader<Text, BytesWritable> {


   private FSDataInputStream filein;
   private ZipInputStream zip;
   private Text key;
   private BytesWritable value;
   //private boolean isFinished = false;
   private long start =0;
   private long end =0;
   private long pos =0;
   //int position = 0;
   private LineReader lr;
   private int position = 0;
   int line = 0;

   @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext context) throws IOException, InterruptedException {

       // File by File

       // The Input split is defined so the data will split by file
       FileSplit fsplit = (FileSplit) inputSplit;

       // Get the filepath that cpntains the file split
       Path fpath = fsplit.getPath();

       // set the configuration
       Configuration c = context.getConfiguration();

       // use zip input stream to read in the split files
       FileSystem fs = fpath.getFileSystem(c);
       filein = fs.open(fpath);
       zip  = new ZipInputStream(filein);




       //       start = fsplit.getStart();
//       end = start + fsplit.getLength();
//       boolean isntfirst = false;
//       if(position != 0){
//           isntfirst = true;
//           filein.seek(position);
//       }
//       lr = new LineReader(filein, c);
//       if(isntfirst){
//           position += lr.readLine(new Text(),0,(int)Math.min((long)Integer.MAX_VALUE, end - start));
//       }
//       this.pos = start;









    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        //We have to read values in from the zip file here and set the keys and values
       // Make sure zip entry isnt null

        // get each entry from the zipinputstream that handled the file and make sure the entry isnt null
       ZipEntry entry = zip.getNextEntry();
       if ( entry == null )
       {
           return false;
       }

        // set the key as the name of the zip entry (filename)
        String keystr = entry.getName();
        key = new Text(entry.getName());


        // Write 2000 bytes at a time from bytearrayoutputstream
       ByteArrayOutputStream bos = new ByteArrayOutputStream();
       byte [] temp = new byte[2000];
       //int flag = 0;
        int intchars = 0;

        //Read as many bytes as possible until 2000 per split until none left
       while (true){
           int bytes_to_read = zip.read(temp,0,2000);
           if (bytes_to_read > 0) {
               bos.write(temp, 0, bytes_to_read);
               //position++;
               //intchars++;
           }

           else {
               break;
           }
       }

        //close the zipinputstream
       zip.closeEntry();

       // set the value to the bytes written, the value will be written as the value in key,value pair passed to mapper
       String v = new String(bos.toByteArray());
        BytesWritable val = new BytesWritable(bos.toByteArray());
        value = new BytesWritable( bos.toByteArray() );;


       return true;









    }

    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
        // your code here
        // the code here depends on what/how you define a split....
       return key;
    }

    @Override
    //public BytesWritable getCurrentValue() throws IOException, InterruptedException {
    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        // your code here
        // the code here depends on what/how you define a split....
       //return val;
       return value;

    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        // let's ignore this one for now
        return 0;
    }

    @Override
    public void close() throws IOException {
        // your code here
        // the code here depends on what/how you define a split....

        //if not null close
        if(filein != null){
            filein.close();
        }
        if(zip != null){
            zip.close();
        }

    }
}

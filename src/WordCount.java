/**
 * Created by prateek on 3/29/14.
 */

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class WordCount {

    static boolean caseSensitive = false;
    static boolean patternSkipping = true;
    private static boolean contextWordDetected = false;
    private static Hashtable queryWord = new Hashtable();
    private static Set queryWordSet;
    private static Iterator queryWordIterator;


    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();


        public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
            String line = (caseSensitive) ? value.toString() : value.toString().toLowerCase();
            int queryBeforeContext = 0;     //We will use this to keep track of queryword occurrences before the appearance of the contextword
            int queryCurrentCount = 0;

            if(patternSkipping){
                line = line.replaceAll("[^\\w\\s]","");
            }


            //Start the actual calculation phase
            while (queryWordIterator.hasNext()) {
                Entry<String,String> entry = (Entry<String,String>)queryWordIterator.next();
                //We need to create a new tokenizer on each iteration
                //Reusing the old tokenizer will make the token point to NULL
                StringTokenizer tokenizer = new StringTokenizer(line);

                while (tokenizer.hasMoreTokens()) {
                    String nextWord = tokenizer.nextToken();
                    //We have found the contextword. Set the detected flag to true
                    if (entry.getKey().equals(nextWord)) {
                        contextWordDetected = true;
                        queryCurrentCount = queryBeforeContext;
                        //Reset the querywords before contextword counter, in case of repeated contextword matches
                        queryBeforeContext = 0;
                    }
                    //We have an instance of the queryword, and we have detected the contextword
                    else if (entry.getValue().equals(nextWord) && contextWordDetected) {
                        queryCurrentCount = queryCurrentCount + 1;
                    }
                    //We need to keep track of any querywords that appear before the contextword. If the contextword is
                    //found sometime later, we should still have an accurate count of how many times the queryword
                    //appeared in this line.
                    else if (entry.getValue().equals(nextWord) && (contextWordDetected == false)) {
                        queryBeforeContext = queryBeforeContext + 1;
                    }

                    //Set how many times we have found the queryword in this line
                    one.set(queryCurrentCount);
                    //The key for the reduce phase OutputCollector will be a concatenation of the contextword and queryword
                    word.set(entry.getKey() + " " + entry.getValue());
                    //Send to Reduce phase
                    output.collect(word, one);
                }

                //After we are finished with the reduce phase, we will need to reset all counters/flags, and
                //go to the next contextword/queryword pairing
                queryBeforeContext = 0;
                queryCurrentCount = 0;
                contextWordDetected = false;
            }

        }
    }

    public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
            int sum = 0;
            while (values.hasNext()) {
                sum += values.next().get();
            }
            output.collect(key, new IntWritable(sum));
        }
    }


    public static void main(String[] args) throws Exception {
        JobConf conf = new JobConf(WordCount.class);
        conf.setJobName("wordcount");

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);

        conf.setMapperClass(Map.class);
        conf.setCombinerClass(Reduce.class);
        conf.setReducerClass(Reduce.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf, new Path(args[0]));

        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        BufferedReader reader = new BufferedReader(new FileReader(args[2]));

        boolean t = true;
        while(t){
            String [] words = reader.readLine().split(" ");
            if(words.equals(null)){
                t = false;
            }
            else{
                queryWord.put(words[0], words[1]);
            }
        }

        //Create a global Set and Iterator so that all Map nodes have access to the contents of the hashtable
        queryWordSet = queryWord.entrySet();
        queryWordIterator = queryWordSet.iterator();

        // Disregard special characters
        conf.setBoolean("analyzer.skip.patterns", true);

        JobClient.runJob(conf);




    }
}
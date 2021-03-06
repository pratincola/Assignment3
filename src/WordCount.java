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
    static Hashtable<String, Boolean> contextWordDetected = new Hashtable();
    static Hashtable<String, Boolean> contextWordDetectedFalser = new Hashtable();
    static Hashtable<String, Integer> queryBeforeContext = new Hashtable();
    static Hashtable<String, Integer> queryBeforeContextFalser = new Hashtable();
    private static Hashtable queryWord = new Hashtable();
    private static Set queryWordSet;
    private static Iterator queryWordIterator;


    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private static String currentContextWord;
        private Text word = new Text();


        public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
            String line = (caseSensitive) ? value.toString() : value.toString().toLowerCase();
            int queryCurrentCount = 0;

            if(patternSkipping){
                line = line.replaceAll("[^\\w\\s]"," ");
                //line = line.replaceAll("'", " ");
            }

            StringTokenizer tokenizer = new StringTokenizer(line);

            while (tokenizer.hasMoreTokens()) {
                queryWordIterator = queryWordSet.iterator();
                String nextWord = tokenizer.nextToken();
                while(queryWordIterator.hasNext()){
                    Entry<String,String> entry = (Entry<String,String>)queryWordIterator.next();
                    currentContextWord = entry.getKey().replaceAll("\\d","");
//                    System.out.println(nextWord + " : entry key " + entry.getKey() + " " + entry.getValue() );

                    if (currentContextWord.equals(nextWord) && (contextWordDetected.get(entry.getKey()) == false)) {
                        contextWordDetected.put(entry.getKey(), true);
                        queryCurrentCount = queryBeforeContext.get(entry.getKey());
                        queryBeforeContext.put(entry.getKey(), 0);
//                        queryCurrentCount = queryBeforeContext;
                        //Reset the querywords before contextword counter, in case of repeated contextword matches
//                        queryBeforeContext = 0;
//                        System.out.println("Found context word: " + nextWord + " count: " + queryBeforeContext);
                    }

                    //We have an instance of the queryword, and we have detected the contextword
                    else if (entry.getValue().equals(nextWord) && contextWordDetected.get(entry.getKey())) {
                        queryCurrentCount = queryCurrentCount + 1;
//                        System.out.println("Found both context & query word: " + nextWord + " count: " +
//                                queryCurrentCount);
                    }

                    //We need to keep track of any querywords that appear before the contextword. If the contextword is
                    //found sometime later, we should still have an accurate count of how many times the queryword
                    //appeared in this line.
                    else if (entry.getValue().equals(nextWord) && (contextWordDetected.get(entry.getKey()) == false)) {
                        Integer tmp = queryBeforeContext.get(entry.getKey()) + 1;
                        queryBeforeContext.put(entry.getKey(), tmp);
//                        queryCurrentCount += queryBeforeContext.get(entry.getKey());

//                        queryBeforeContext = queryBeforeContext + 1;
//                        queryCurrentCount += queryBeforeContext;

//                        System.out.println("found query word before context " + nextWord + " count: " +
//                                queryBeforeContext);
                    }



                    if(contextWordDetected.get(entry.getKey())){
                        one.set(queryCurrentCount);
                    }
                    else
                        one.set(0);

                    word.set(currentContextWord + " " + entry.getValue());
//                    word.set(nextWord);
                    output.collect(word, one);


                    queryCurrentCount = 0;

                }


            }
            queryBeforeContext.putAll(queryBeforeContextFalser);
            contextWordDetected.putAll(contextWordDetectedFalser);

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

        String words;
        int i = 0;
        // TODO: Take care of line feed at the end of the file
        while((words = reader.readLine())!= null ){
            String [] word = words.split(" ") ;
            queryWord.put(word[0].toLowerCase() + String.valueOf(i),word[1].toLowerCase());
            contextWordDetected.put(word[0].toLowerCase() + String.valueOf(i), false);
            contextWordDetectedFalser.put(word[0].toLowerCase() + String.valueOf(i), false);
            queryBeforeContext.put(word[0].toLowerCase() + String.valueOf(i), 0);
            queryBeforeContextFalser.put(word[0].toLowerCase() + String.valueOf(i), 0);

            i++;
        }

        //Create a global Set and Iterator so that all Map nodes have access to the contents of the hashtable
        queryWordSet = queryWord.entrySet();
        queryWordIterator = queryWordSet.iterator();



        // Disregard special characters
        conf.setBoolean("analyzer.skip.patterns", true);

        JobClient.runJob(conf);




    }
}
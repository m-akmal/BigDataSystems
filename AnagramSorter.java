/**
 *  This program groups all the anagrams in a given list of words and then
 *  orders the sorted anagram string in a descending order of size of each.
 *  Group 23 : Big Data Systems (CS744) Fall 2017
 *  Authors: Tarun, Rohit, Pavan
 */
import org.apache.hadoop.mapred.MapReduceBase;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.io.IntWritable;

import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.conf.Configuration;


public class AnagramSorter {
  public static class AnagramMapper extends MapReduceBase
       implements Mapper<LongWritable, Text, Text, Text>{
    @Override
    /**
     * This is the initial map function which creates (sortedvalue, value) type
     * tuples for every word in the input file. This is useful for detecting
     * anagrams because all anagrams have the same text on being sorted.
     * @param  LongWritable          key           The key being read.
     * @param  Text                  value         The value of the key
     * @param  OutputCollector<Text,Text>         results       To store the
     *                                                          output results
     * @param  Reporter              reporter      [default]
     * @throws IOException           [default]
     */
    public void map(LongWritable key, Text value, OutputCollector<Text, Text> results, Reporter reporter
                    ) throws IOException{
        Text word = new Text();
        Text sorted_word = new Text();
        String inp_string, sorted_string;
        inp_string = value.toString();
        sorted_string = sort(inp_string);
        word.set(inp_string);
        sorted_word.set(sorted_string);
        results.collect(sorted_word, word);

    }

    /**
     * Sort the given input string and return the output string
     * @param  String input         input string
     * @return        sorted output string is returned.
     */
    protected String sort(String input) {
      char[] cs = input.toCharArray();
      Arrays.sort(cs);
      return new String(cs);
    }
  }

  public static class AnagramReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text>{
           @Override
           /**
            * This reduce function accumulates all the anagrams and creates a
            * single comma separated string of all anagrams followed by "~" and
            * the number of anagrams .
            * @param  Text                  key           sorted word
            * @param  Iterator<Text>        values        list of all words that
            * have the same sorted_word.
            * @param  OutputCollector<Text,Text>         results       Output
            * values in the form of (anagramPairs~n,"")
            * @param  Reporter              reporter      default
            * @throws IOException           default
            */
           public void reduce(Text key, Iterator<Text> values,
                              OutputCollector<Text, Text> results,
                              Reporter reporter
                              ) throws IOException {
              Text outputKey = new Text();
              Text outputVal = new Text();
              //IntWritable count = new IntWritable();
              Set<Text> uniques = new HashSet<Text>();
              int size = 0;
              StringBuilder builder = new StringBuilder();

              // Create anagram string by grouping the anagrams
              while (values.hasNext()) {
                Text value = values.next();
                if (uniques.add(value)) {
                  size++;
                  builder.append(value.toString());
                  builder.append(',');
                }
              }
              builder.setLength(builder.length() - 1);

              if (size > 1) {
                // Add the number of words in the anagram string to output.
                builder.append('~');
                builder.append(size);
                outputKey.set(builder.toString());
                outputVal.set("");
                results.collect(outputKey, outputVal);
              }

           }
         }

  public static class AnagramSortMapper extends MapReduceBase
     implements Mapper<LongWritable, Text, Text, Text>{
       @Override
       /**
        * This is the map function of our second job which creates tuples of
        * (n,anagrams) tuples for all anagram pairs found in previous reduce
        * task whose output was saved as (anagrams~n,""). This is aimed at
        * sorting the anagram pairs based on their count n.
        * @param  LongWritable          key           The input key
        * @param  Text                  value         Value of the input key.
        * @param  OutputCollector<Text,Text>         results      Output saved
        * as n,anagramPairs
        * @param  Reporter              reporter      default
        * @throws IOException           default
        */
       public void map(LongWritable key, Text value, OutputCollector<Text, Text> results, Reporter reporter
                       ) throws IOException {
                Text new_key = new Text();
                Text new_val = new Text();
                String val = value.toString();
                // Extract the count n from anagram~n
                String[] parts = val.split("~");
                new_key.set(parts[1]);
                new_val.set(parts[0]);
                // Save as (n, anagram)
                results.collect(new_key, new_val);
            }
     }

 public static class AnagramSortReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text>{
    @Override
    /**
     *  Order the anagram strings based on the size of string which is returned
     *  as a key by previous map step. Add a newline character between every
     *  anagram string.
     * @param  Text                  key           The size of anagram string
     * returned by the map step
     * @param  Iterator<Text>        values        The list of anagram strings
     * of the same size.
     * @param  OutputCollector<Text, Text>         results       Final output.
     * @param  Reporter              reporter      default
     * @throws IOException           default
     */
    public void reduce(Text key, Iterator<Text> values,
                       OutputCollector<Text, Text> results,
                       Reporter reporter
                       ) throws IOException {

            Text outputKey = new Text();
       			Text outputValue = new Text();
            String output_val = "";
            while (values.hasNext()){
              Text val = values.next();
              String str_val = val.toString();
              str_val = str_val.trim();
              str_val = str_val.replaceAll(","," ");
              output_val += str_val + "\n";
            }
            outputKey.set("");
      			outputValue.set(output_val);
      			results.collect(outputKey, outputValue);
        }
    }


  public static void main(String[] args) throws Exception {
    JobConf conf = new JobConf(AnagramSorter.class);

    // Start the first job for finding anagrams
    conf.setJobName("anagam find");
    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(Text.class);
    conf.setMapperClass(AnagramMapper.class);
    conf.setReducerClass(AnagramReducer.class);
    conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
    FileInputFormat.setInputPaths(conf, new Path(args[0]));
    FileOutputFormat.setOutputPath(conf, new Path("/intermediate"));
    conf.setJar("ac.jar");
    JobClient.runJob(conf);

    // Start the second job for sorting the anagrams.
    conf.setJobName("anagam sort");
    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(Text.class);
    // For descending order
    conf.setOutputKeyComparatorClass(LongWritable.DecreasingComparator.class);
    conf.setMapperClass(AnagramSortMapper.class);
    conf.setReducerClass(AnagramSortReducer.class);
    FileInputFormat.setInputPaths(conf, new Path("/intermediate"));
    FileOutputFormat.setOutputPath(conf, new Path(args[1]));
    conf.setJar("ac.jar");
		JobClient.runJob(conf);
  }
}

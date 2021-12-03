import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable> {
        private HashMap<String, Integer> wordCount;
        private TreeMap<Integer, String> tmap;
        private Text word = new Text();

        // Make a hashset of stop words to filer out
        private String[] stopWordsList = {"i", "me", "my", "myself", "we", "our", "ours", "ourselves", "you", "your", "yours", "yourself", "yourselves", "he", "him", "his", "himself", "she", "her", "hers", "herself", "it", "its", "itself", "they", "them", "their", "theirs", "themselves", "what", "which", "who", "whom", "this", "that", "these", "those", "am", "is", "are", "was", "were", "be", "been", "being", "have", "has", "had", "having", "do", "does", "did", "doing", "a", "an", "the", "and", "but", "if", "or", "because", "as", "until", "while", "of", "at", "by", "for", "with", "about", "against", "between", "into", "through", "during", "before", "after", "above", "below", "to", "from", "up", "down", "in", "out", "on", "off", "over", "under", "again", "further", "then", "once", "here", "there", "when", "where", "why", "how", "all", "any", "both", "each", "few", "more", "most", "other", "some", "such", "no", "nor", "not", "only", "own", "same", "so", "than", "too", "very", "s", "t", "can", "will", "just", "don", "should", "now"};
        private HashSet<String> stopwords = new HashSet<String>(Arrays.asList(stopWordsList));
        private ArrayList<Map.Entry<String, Integer>> wordCountList;

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            wordCount = new HashMap<String, Integer>();
            tmap = new TreeMap<Integer, String>();
            wordCountList= new ArrayList<Map.Entry<String, Integer>>();
        }

        @Override
        public void map(Object term, Text value, Context context
        ) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                String word_string = word.toString().toLowerCase();
                // check if in stop words
                if(stopwords.contains(word_string)) {
                    continue;
                }
                // increment word count by 1
                if(wordCount.containsKey(word_string)) {
                    wordCount.replace(word_string, wordCount.get(word_string) + 1);
                }
                else {
                    wordCount.put(word_string, 1);
                }
            }
        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
            // Transfers items from the HashMap to an ArrayList
            for (HashMap.Entry<String, Integer> wordCounts: wordCount.entrySet()) {
                wordCountList.add(new java.util.AbstractMap.SimpleEntry<>(wordCounts.getKey(), wordCounts.getValue()));
            }
            
            // Sort all the key, value pairs by the value
            Collections.sort(wordCountList, new Comparator<Map.Entry<String, Integer> >() {
                // compare method to compare by values
                public int compare(Map.Entry<String, Integer> o1, Map.Entry<String, Integer> o2) {
                    return (o2.getValue()).compareTo(o1.getValue());
                }
            });

            // Write the top 5 items
            int i = 0;
            for (Map.Entry<String, Integer> item: wordCountList) {
                context.write(new Text(item.getKey()), new IntWritable(item.getValue()));
                i++;
                if(i >= 5) {
                    break;
                }
            }
        }
    }

    public static class IntSumReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {
        private HashMap<String, Integer> wordCount;
        private ArrayList<Map.Entry<String, Integer>> wordCountList;
        
        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            wordCount = new HashMap<String, Integer>();
            wordCountList= new ArrayList<Map.Entry<String, Integer>>();
        }

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            // increment word count by the mapper count
            for(IntWritable count : values) {
                if(wordCount.containsKey(key.toString())) {
                    wordCount.replace( key.toString(), wordCount.get(key.toString()) + count.get());
                } 
                else {
                    wordCount.put(key.toString(), count.get());
                }
            }
        }
        
        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
            // Transfers items from the HashMap to an ArrayList
            for (HashMap.Entry<String, Integer> wordCounts: wordCount.entrySet()) {
                wordCountList.add(new AbstractMap.SimpleEntry<>(wordCounts.getKey(), wordCounts.getValue()));
            }

            // Sort all the key, value pairs by the value
            Collections.sort(wordCountList, new Comparator<Map.Entry<String, Integer> >() {
                // compare method to compare by values
                public int compare(Map.Entry<String, Integer> o1, Map.Entry<String, Integer> o2) {
                    return (o2.getValue()).compareTo(o1.getValue());
                }
            });

            // Write the top 5 items
            int i = 0;
            for (Map.Entry<String, Integer> item: wordCountList) {
                context.write(new Text(item.getKey()), new IntWritable(item.getValue()));
                i++;
                if(i >= 5) {
                    break;
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setNumReduceTasks(1);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
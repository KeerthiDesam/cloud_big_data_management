package hadoop;

/**
 * @author Chandrashekar Akkenapally
 *
 */

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Hadoop {
	static Map<String, Integer> sortMap(Map<String, Integer> unsortMap) {
		Map<String, Integer> linkedHashMap = new LinkedHashMap<String, Integer>();
		List<Map.Entry<String, Integer>> list = new ArrayList<Map.Entry<String, Integer>>(unsortMap.entrySet());
		// Sorting the list we created from unsorted Map
		Collections.sort(list, new Comparator<Map.Entry<String, Integer>>() {
			public int compare(Map.Entry<String, Integer> o1, Map.Entry<String, Integer> o2) {
				// sorting in descending order
				return o2.getKey().equals(o1.getKey()) ? o1.getValue().compareTo(o2.getValue())
						: o1.getKey().compareTo(o2.getKey());
			}
		});
		for (Map.Entry<String, Integer> entry : list) {
			linkedHashMap.put(entry.getKey(), entry.getValue());
		}
		return linkedHashMap;
	}

	static Map<String, Double> sortAvgWordLengthMap(Map<String, Double> unsortMap) {
		Map<String, Double> linkedHashMap = new LinkedHashMap<String, Double>();
		List<Map.Entry<String, Double>> list = new ArrayList<Map.Entry<String, Double>>(unsortMap.entrySet());
		// Sorting the list we created from unsorted Map
		Collections.sort(list, new Comparator<Map.Entry<String, Double>>() {
			public int compare(Map.Entry<String, Double> o1, Map.Entry<String, Double> o2) {
				// sorting in descending order
				return o1.getKey().compareTo(o2.getKey());
			}
		});
		for (Map.Entry<String, Double> entry : list) {
			linkedHashMap.put(entry.getKey(), entry.getValue());
		}
		return linkedHashMap;
	}

	static Map<String, Integer> getTop200Words(Map<String, Integer> unsortMap) {
		Map<String, Integer> linkedHashMap = new LinkedHashMap<String, Integer>();
		List<Map.Entry<String, Integer>> list = new ArrayList<Map.Entry<String, Integer>>(unsortMap.entrySet());
		// Sorting the list we created from unsorted Map
		Collections.sort(list, new Comparator<Map.Entry<String, Integer>>() {
			public int compare(Map.Entry<String, Integer> o1, Map.Entry<String, Integer> o2) {
				// sorting in descending order
				return o2.getValue().compareTo(o1.getValue());
			}
		});
		for (int nthItem = 0; nthItem < list.size(); nthItem++) {
			if (nthItem >= 200)
				break;
			linkedHashMap.put(list.get(nthItem).getKey(), list.get(nthItem).getValue());
		}

		return linkedHashMap;
	}

	public static class WordCountMapper extends Mapper<Object, Text, Text, IntWritable> {
		private Text word = new Text();

		public void map(Object key, Text text, Context context) throws IOException, InterruptedException {
			ArrayList<String> listOfWords = AliceAdventures.removeStopWords(text.toString());
			HashMap<String, Integer> wordToFrequency = AliceAdventures.wordCount(listOfWords);
			for (String eachKey : wordToFrequency.keySet()) {
				word.set(eachKey);
				context.write(word, new IntWritable(wordToFrequency.get(eachKey)));
			}
		}
	}

	public static class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();
		public Map<String, Integer> map = new LinkedHashMap<String, Integer>();

		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			map.put(key.toString(), sum);
		}

		public void cleanup(Context context) throws IOException, InterruptedException {
			Map<String, Integer> sortedMap = sortMap(map);
			for (Map.Entry<String, Integer> entry : sortedMap.entrySet()) {
				context.write(new Text(entry.getKey()), new IntWritable(entry.getValue()));
			}
		}
	}

	public static class Top200Mapper extends Mapper<Object, Text, Text, IntWritable> {
		private Text word = new Text();

		public void map(Object key, Text text, Context context) throws IOException, InterruptedException {
			ArrayList<String> listOfWords = AliceAdventures.removeStopWords(text.toString());
			HashMap<String, Integer> wordToFrequency = AliceAdventures.wordCount(listOfWords);
			for (String eachKey : wordToFrequency.keySet()) {
				word.set(eachKey);
				context.write(word, new IntWritable(wordToFrequency.get(eachKey)));
			}
		}
	}

	public static class Top200Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();
		public Map<String, Integer> map = new LinkedHashMap<String, Integer>();

		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			map.put(key.toString(), sum);
		}

		public void cleanup(Context context) throws IOException, InterruptedException {
			Map<String, Integer> sortedMap = getTop200Words(map);
			for (Map.Entry<String, Integer> entry : sortedMap.entrySet()) {
				context.write(new Text(entry.getKey()), new IntWritable(entry.getValue()));
			}
		}
	}

	public static class AvgWordLengthPerCharMapper extends Mapper<Object, Text, Text, WordStatsWritable> {
		private Text word = new Text();

		public void map(Object key, Text text, Context context) throws IOException, InterruptedException {
			ArrayList<String> listOfWords = AliceAdventures.removeStopWords(text.toString());
			HashMap<Character, WordStats> characterToStats = AliceAdventures.averageLengthWords(listOfWords);
			for (Character eachKey : characterToStats.keySet()) {
				word.set(eachKey.toString());
				context.write(word, new WordStatsWritable(characterToStats.get(eachKey)));
			}
		}
	}

	public static class AvgWordLengthPerCharReducer extends Reducer<Text, WordStatsWritable, Text, DoubleWritable> {
		public Map<String, Double> map = new LinkedHashMap<String, Double>();

		public void reduce(Text key, Iterable<WordStatsWritable> values, Context context)
				throws IOException, InterruptedException {
			long totalWordLength = 0;
			int totalFrequency = 0;
			for (WordStatsWritable val : values) {
				totalWordLength += val.length.get();
				totalFrequency += val.frequency.get();
			}
			map.put(key.toString(),  ((double) totalWordLength / totalFrequency));
		}

		public void cleanup(Context context) throws IOException, InterruptedException {
			Map<String, Double> sortedMap = sortAvgWordLengthMap(map);
			for (Map.Entry<String, Double> entry : sortedMap.entrySet()) {
				context.write(new Text(entry.getKey()), new DoubleWritable(entry.getValue()));
			}
		}
	}

	public static Job configureWordCountJob() throws IOException {
		Configuration conf = new Configuration();
		conf.set("mapred.textoutputformat.separator", ", "); 
		Job job = new Job(conf, "Word Count");
		job.setJarByClass(Assignment1.class);
		job.setMapperClass(WordCountMapper.class);
		job.setReducerClass(WordCountReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		return job;
	}

	public static Job configureTop200Job() throws IOException {
		Configuration conf = new Configuration();
		conf.set("mapred.textoutputformat.separator", ", "); 
		Job job = new Job(conf, "Top200 Words");
		job.setJarByClass(Assignment1.class);
		job.setMapperClass(Top200Mapper.class);
		job.setReducerClass(Top200Reducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		return job;
	}

	public static Job configureAvgLenPerChar() throws IOException {
		Configuration conf = new Configuration();
		conf.set("mapred.textoutputformat.separator", ", "); 
		Job job = new Job(conf, "Avg Word length of Character");
		job.setJarByClass(Assignment1.class);
		job.setMapperClass(AvgWordLengthPerCharMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(WordStatsWritable.class);
		job.setReducerClass(AvgWordLengthPerCharReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		return job;
	}

	public static void main(String[] args) throws Exception {
		Job wordCountJob = configureWordCountJob();
		FileInputFormat.addInputPath(wordCountJob, new Path(args[0]));
		FileOutputFormat.setOutputPath(wordCountJob, new Path(args[1] + "WordCount"));
		wordCountJob.waitForCompletion(true);
		if (!wordCountJob.isSuccessful()) {
			System.exit(0);
		}
		Job top200Job = configureTop200Job();
		FileInputFormat.addInputPath(top200Job, new Path(args[0]));
		FileOutputFormat.setOutputPath(top200Job, new Path(args[1] + "Top200"));
		top200Job.waitForCompletion(true);
		if (!wordCountJob.isSuccessful()) {
			System.exit(0);
		}
		Job avgLenPerChar = configureAvgLenPerChar();
		FileInputFormat.addInputPath(avgLenPerChar, new Path(args[0]));
		FileOutputFormat.setOutputPath(avgLenPerChar, new Path(args[1] + "AvgWordLength"));
		System.exit(avgLenPerChar.waitForCompletion(true) ? 0:1);
	}
}

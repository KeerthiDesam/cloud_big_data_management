package hadoop;


/**
 * @author Chandrashekar Akkenapally
 *
 */

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.StopFilter;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

class WordStatsWritable implements Writable{
	LongWritable length;
	IntWritable frequency;
	
	WordStatsWritable(){
		length = new LongWritable();
		frequency = new IntWritable();
	}
	
	WordStatsWritable(LongWritable length, IntWritable frequency){
		this.length = length;
		this.frequency = frequency;
	}
	
	WordStatsWritable(WordStats wordStats){
		this.length = new LongWritable(wordStats.length);
		this.frequency =  new IntWritable(wordStats.frequency);
	}
	
	public String toString() {
		return String.format("%d %d",this.length,this.frequency);
	}

	@Override
	public void readFields(DataInput input) throws IOException {
		length.readFields(input);
		frequency.readFields(input);
	}

	@Override
	public void write(DataOutput output) throws IOException {
		// TODO Auto-generated method stub
		length.write(output);
		frequency.write(output);
	}	
}

class WordStats{
	long length;
	int frequency;
	WordStats(long length, int frequency){
		this.length = length;
		this.frequency = frequency;
	}
	public String toString() {
		return String.format("%d %d",this.length,this.frequency);
	}	
}

public class AliceAdventures {
	static final String stopWords = "i,me,my,myself,we,our,ours,ourselves,you,your,yours,yourself,yourselves,he,him,his,himself,she,her,hers,herself,it,its,itself,they,them,their,theirs,themselves,what,which,who,whom,this,that,these,those,am,is,are,was,were,be,been,being,have,has,had,having,do,does,did,doing,a,an,the,and,but,if,or,because,as,until,while,of,at,by,for,with,about,against,between,into,through,during,before,after,above,below,to,from,up,down,in,out,on,off,over,under,again,further,then,once,here,there,when,where,why,how,all,any,both,each,few,more,most,other,some,such,no,nor,not,only,own,same,so,than,too,very,s,t,can,will,just,don,should,now";

	public static ArrayList<String> removeStopWords(String textFile) throws IOException  {
		final List<String> stopWordsList = Arrays.asList(stopWords.split(","));
		final CharArraySet stopSet = new CharArraySet(stopWordsList, true);
		StandardTokenizer standardTokenizer = new StandardTokenizer();
		standardTokenizer.setReader(new StringReader(textFile));
		StopFilter tokenStream = new StopFilter(standardTokenizer, stopSet);
		ArrayList<String> sb = new ArrayList<String>();
		CharTermAttribute charTermAttribute = tokenStream.addAttribute(CharTermAttribute.class);
		tokenStream.reset();
		while (tokenStream.incrementToken()) {
			String term = charTermAttribute.toString();
			sb.add(term.toLowerCase());
		}
		tokenStream.close();
		standardTokenizer.close();
		return sb;
	}

	public static HashMap<String,Integer> wordCount(ArrayList<String> listOfWords) {
		int k = 0;
		HashMap<String, Integer> wordToFreq = new HashMap<>();
		for (String eachWord : listOfWords) {
			if (wordToFreq.containsKey(eachWord)) {
				k = wordToFreq.get(eachWord);
				k++;
				wordToFreq.replace(eachWord, k);
				continue;
			}
			wordToFreq.put(eachWord, 1);
		}
//		System.out.println(wordToFreq);
		return wordToFreq;
	}
	
	public static HashMap<Character, WordStats> averageLengthWords(ArrayList<String> listOfWords) {
		WordStats k = new WordStats(0,0); 
		HashMap<Character, WordStats> charToFreq = new HashMap<>();
		for (String eachWord : listOfWords) {
			Character firstChar = eachWord.charAt(0);
			if (charToFreq.containsKey(firstChar)) {
				k = charToFreq.get(firstChar);
				k.length+=eachWord.length();
				k.frequency++;
				continue;
			}
			charToFreq.put(eachWord.charAt(0), new WordStats(eachWord.length(), 1));
		}
		System.out.println(charToFreq);
		return charToFreq;
	}
	
}

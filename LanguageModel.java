import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

public class LanguageModel {
	public static class Map extends Mapper<LongWritable, Text, Text, Text> {

		int threshold;

		@Override
		public void setup(Context context) {
			// how to get the threashold parameter from the configuration?
			Configuration conf = context.getConfiguration();
			threshold = conf.getInt("threshold",20);
		}

		
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			if((value == null) || (value.toString().trim()).length() == 0) {
				return;
			}
			//this is cool\t20
			String line = value.toString().trim();
			
			String[] wordsPlusCount = line.split("\t");
			if(wordsPlusCount.length < 2) {
				return;
			}
			
			String[] words = wordsPlusCount[0].split("\\s+");
			int count = Integer.valueOf(wordsPlusCount[1]);

			//how to filter the n-gram lower than threashold
			if(count < threshold)
				return;
			//this is --> cool = 20

			//outputkey: this is
			//outputvalue: cool=20
			StringBuilder sb = new StringBuilder();
			for(int i = 0 ; i < words.length - 1; ++ i){
				sb.append(words[i]);
				sb.append(" ");
			}
			String outputkey = sb.toString().trim();
			String outputvalue = words[words.length - 1];

			//write key-value to reducer
			if(! (outputkey.length() < 1 || outputkey == null))
				context.write(new Text(outputkey), new Text(outputvalue] + "=" + count));

		}
	}

	public static class Reduce extends Reducer<Text, Text, DBOutputWritable, NullWritable> {

		int n;
		// get the n parameter from the configuration
		@Override
		public void setup(Context context) {
			Configuration conf = context.getConfiguration();
			n = conf.getInt("n", 5);
		}

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			//this is, <girl = 50, boy = 60> ==>
			TreeMap<Integer, List<String>>  tm = new TreeMap<>(Collections.reverseOrder());
			for(Text value : values){
				String cur = value.toString().trim();
				String wordvalue = cur.split("=")[0].trim();
				int count = Integer.parseInt(cur.split("=")[1].trim());
				if(tm.containsKey(count)){
					tm.get(count).add(cur);
				}else
				{
					List<String> list = new ArrayList<>();
					list.add(cur);
					tm.put(count, list);
				}
			}
			//==> <50, <girl, bird>> <60, <boy,...>>

			//iterate treemap and select top n elements store into DB
			Iterator<Integer> it = tm.keySet().iterator();
			for(int j = 0; j < n && it.hasNext();j++){
				int count = it.next();
				List<String> words = tm.get(count);
				for(String cur : words){
					context.write(new DBOutputWritable(key.toString(), cur, count),NullWritable.get();
					j++;
				}
			}
		}
	}
}

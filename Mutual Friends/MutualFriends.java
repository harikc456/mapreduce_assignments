
import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class MutualFriends {

	public static class MapForForFriends extends Mapper<LongWritable, Text, Text, Text> {
		public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException {
			String line = value.toString();
			String[] users = line.split(" -> ");
			String[] users1 = users[1].split(" ");
			for(int i =0;i<users1.length;i++) {
				String[] friends = new String[2];
				friends[0] = users[0];
				friends[1] = users1[i];
				Arrays.parallelSort(friends);
				con.write(new Text(friends[0]+" "+friends[1]), new Text(users[1]));
			}
		}
	}

	public static class ReduceForFriends extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context con)
				throws IOException, InterruptedException {
			String[] result = new String[10];
			int index = 0;
			int count = 0;
			String result_string = new String("");
			String[] friends = new String[2];
			for (Text value : values) {
				friends[count++]= value.toString();
			}

			String[] friends1 = friends[1].split(" ");
			String[] friends0 = friends[0].split(" ");
			for(int i = 0; i<friends0.length ;i++) {
				for(int j = 0; j<friends1.length ;j++) {
					if(friends0[i].contentEquals(friends1[j])) {
						result[index++] = friends0[i];
					}
				}
			}
			for (int i=0;i<result.length;i++) {
				if(result[i] != null) {
					result_string += result[i];
				}
			}
			con.write(key, new Text(result_string));
		}
	}

	public static void main(String args[]) throws Exception {
		Configuration c = new Configuration();
		String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
		Path input = new Path(files[0]);
		Path output = new Path(files[1]);
		Job j = new Job(c, "Common_friends");
		j.setJarByClass(MutualFriends.class);
		j.setMapperClass(MapForForFriends.class);
		j.setReducerClass(ReduceForFriends.class);
		j.setOutputKeyClass(Text.class);
		j.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(j, input);
		FileOutputFormat.setOutputPath(j, output);
		System.exit(j.waitForCompletion(true) ? 0 : 1);

	}
}

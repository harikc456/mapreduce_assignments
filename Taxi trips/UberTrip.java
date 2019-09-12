
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class UberTrip {

	public static class MapForWordCount extends Mapper<LongWritable, Text, Text, Text> {
		public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException {
			String line = value.toString();
			if(key.get()==0 && line.contains("dispatching_base_number")) {
				return;
			}
			else {
				String[] words = line.split(",");
				Text outputKey = new Text(words[0]);
				Text outputValue = new Text(words[1]+":"+words[3]);
				con.write(outputKey, outputValue);
			}
		}
	}

	public static class ReduceForWordCount extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text word, Iterable<Text> values, Context con)
				throws IOException, InterruptedException {
			int max = 0;
			String date = new String("");
			for (Text value : values) {
				String[] trip_info = value.toString().split(":");
				if(Integer.parseInt(trip_info[1]) > max) {
					max = Integer.parseInt(trip_info[1]);
					date = trip_info[0];
				}

			}
			con.write(word, new Text(date+':'+max));
		}
	}

	public static void main(String args[]) throws Exception {
		Configuration c = new Configuration();
		String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
		Path input = new Path(files[0]);
		Path output = new Path(files[1]);
		Job j = new Job(c, "ubertrip");
		j.setJarByClass(UberTrip.class);
		j.setMapperClass(MapForWordCount.class);
		j.setReducerClass(ReduceForWordCount.class);
		j.setOutputKeyClass(Text.class);
		j.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(j, input);
		FileOutputFormat.setOutputPath(j, output);
		System.exit(j.waitForCompletion(true) ? 0 : 1);

	}
}

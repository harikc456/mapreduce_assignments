
import java.io.IOException;
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
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class QuizEval {

	public static class MapForWordCount extends Mapper<LongWritable, Text, Text, IntWritable> {
		public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException {
			String line = value.toString();
			String[] words = line.split("\t");
			Text outputKey = new Text(words[0]);
			IntWritable outputValue = new IntWritable(new Integer(words[1]));
			con.write(outputKey, outputValue);
		}
	}

	public static class ReduceForWordCount extends Reducer<Text, IntWritable, Text, FloatWritable> {
		public void reduce(Text word, Iterable<IntWritable> values, Context con)
				throws IOException, InterruptedException {
			int sum = 0;
			int count = 0;
			for (IntWritable value : values) {
				sum += value.get();
				count += 1;
			}
			con.write(word, new FloatWritable(sum));
			con.write(new Text("Average:"), new FloatWritable(sum/count));
		}
	}

	public static void main(String args[]) throws Exception {
		Configuration c = new Configuration();
		String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
		Path input = new Path(files[0]);
		//String[] paths = new String[5];
		Path output = new Path(files[1]);
		Job j = new Job(c, "quiz");
		j.setJarByClass(QuizEval.class);
		j.setMapperClass(MapForWordCount.class);
		j.setReducerClass(ReduceForWordCount.class);
		j.setOutputKeyClass(Text.class);
		j.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(j, input);
		/*MultipleInputs.addInputPath(j, new Path(files[0]), TextInputFormat.class,MapForWordCount.class);
		MultipleInputs.addInputPath(j, new Path(files[1]), TextInputFormat.class,MapForWordCount.class);
		MultipleInputs.addInputPath(j, new Path(files[2]), TextInputFormat.class,MapForWordCount.class);
		MultipleInputs.addInputPath(j, new Path(files[3]), TextInputFormat.class,MapForWordCount.class);
		MultipleInputs.addInputPath(j, new Path(files[4]), TextInputFormat.class,MapForWordCount.class);
		*/
		FileOutputFormat.setOutputPath(j, output);
		System.exit(j.waitForCompletion(true) ? 0 : 1);

	}
}

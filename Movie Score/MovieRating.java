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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class MovieRating {
public static void main(String [] args) throws Exception
{
Configuration c=new Configuration();
String[] files=new GenericOptionsParser(c,args).getRemainingArgs();
Path input=new Path(files[0]);
Path output=new Path(files[1]);
Job j=new Job(c,"movierating");
j.setJarByClass(MovieRating.class);
j.setMapperClass(MapForWordCount.class);
j.setReducerClass(ReduceForWordCount.class);
j.setOutputKeyClass(Text.class);
j.setOutputValueClass(Text.class);
FileInputFormat.addInputPath(j, input);
FileOutputFormat.setOutputPath(j, output);
System.exit(j.waitForCompletion(true)?0:1);
}
public static class MapForWordCount extends Mapper<LongWritable, Text, Text, Text>{
public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException
{
String line = value.toString();
String[] words=line.split("\n");
for(String word: words )
{
	String[] movie_info = word.split("\t");
	if(movie_info[1].equals("score")) {
		continue;
	}
    Text outputKey = new Text(movie_info[3]);
    Text outputValue = new Text(movie_info[0]+"\t"+movie_info[1]);
    con.write(outputKey, outputValue);
}
}
}
public static class ReduceForWordCount extends Reducer<Text, Text, Text, Text>
{
public void reduce(Text word, Iterable<Text> values, Context con) throws IOException, InterruptedException
{
float max = 0;
String moviename = new String(" ");
   for(Text value : values)
   {
	    String[] ratings = value.toString().split("\t");
		if(Float.parseFloat(ratings[1]) > max) {
			max = Float.parseFloat(ratings[1]);
			moviename = ratings[0];
		}
   }
   con.write(word,new Text(moviename+"\t"+max));
}
}
}
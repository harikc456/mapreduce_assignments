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
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class InvertedIndex {
public static void main(String [] args) throws Exception
{
Configuration c=new Configuration();
String[] files=new GenericOptionsParser(c,args).getRemainingArgs();
Path input=new Path(files[0]);
Path output=new Path(files[1]);
Job j=new Job(c,"InvertedIndex");
j.setJarByClass(InvertedIndex.class);
j.setMapperClass(MapForIndex.class);
j.setReducerClass(ReduceForIndex.class);
j.setOutputKeyClass(Text.class);
j.setOutputValueClass(Text.class);
FileInputFormat.addInputPath(j, input);
FileOutputFormat.setOutputPath(j, output);
System.exit(j.waitForCompletion(true)?0:1);
}
public static class MapForIndex extends Mapper<LongWritable, Text, Text, Text>{
public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException
{
String line = value.toString();
String[] words = line.split(" ");
for(String word:words) {
    	Text outputKey = new Text(word);
    	String filename = ((FileSplit)con.getInputSplit()).getPath().toString();
    	Text outputValue = new Text(filename+"#"+1);
    	con.write(outputKey,outputValue);
}
}
}
public static class ReduceForIndex extends Reducer<Text, Text, Text, Text>
{
public void reduce(Text word, Iterable<Text> values, Context con) throws IOException, InterruptedException
{
float word_freq = 0;
String filenames= new String("");
   for(Text value : values)
   {
	    String[] index_values = value.toString().split("#");
	    word_freq += Integer.parseInt(index_values[1]);
	    if(filenames.contains(index_values[0])==false) {
	    	filenames = filenames+"\t"+index_values[0];
	    }
   }
   con.write(word,new Text(word_freq+"\t"+filenames));
}
}
}
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

public class SalaryAverage {
public static void main(String [] args) throws Exception
{
Configuration c=new Configuration();
String[] files=new GenericOptionsParser(c,args).getRemainingArgs();
Path input=new Path(files[0]);
Path output=new Path(files[1]);
Job j=new Job(c,"Salary_Average");
j.setJarByClass(SalaryAverage.class);
j.setMapperClass(MapForWordCount.class);
j.setReducerClass(ReduceForWordCount.class);
j.setOutputKeyClass(Text.class);
j.setOutputValueClass(FloatWritable.class);
FileInputFormat.addInputPath(j, input);
FileOutputFormat.setOutputPath(j, output);
System.exit(j.waitForCompletion(true)?0:1);
}
public static class MapForWordCount extends Mapper<LongWritable, Text, Text, FloatWritable>{
public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException
{
String line = value.toString();
String[] words=line.split("\n");
for(String word: words )
{
	String[] employee_info = word.split(" ");
    Text outputKey = new Text(employee_info[2]);
    float salary = new Float(employee_info[8]);
    FloatWritable outputValue = new FloatWritable(salary);
    con.write(outputKey, outputValue);
}
}
}
public static class ReduceForWordCount extends Reducer<Text, FloatWritable, Text, FloatWritable>
{
public void reduce(Text word, Iterable<FloatWritable> values, Context con) throws IOException, InterruptedException
{
int sum = 0;
int count = 0;
   for(FloatWritable value : values)
   {
		sum += value.get();
		count += 1;
   }
   con.write(word, new FloatWritable(sum/count));
   con.write(new Text("Total"), new FloatWritable(sum));
}
}
}
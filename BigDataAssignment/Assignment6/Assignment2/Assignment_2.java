import java.io.IOException;
import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Assignment_2 {
	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
		// private final static IntWritable one = new IntWritable(1);
		// private Text word = new Text();

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			int count = 0, sum = 0, found = 0;
			String No_of_Medals = "", Year = "";

			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);
			while (tokenizer.hasMoreTokens()) {

				count = count + 1;

				if (tokenizer.nextToken("\t").equals("India")) {
					Year = tokenizer.nextToken("\t");

					sum = 0;
					found = 1;
					// context.write(new Text(Country_name), new
					// IntWritable(Integer.parseInt(Medal)) );
				}

				if (found == 1) {

					if (count == 5) {
						No_of_Medals = tokenizer.nextToken("\t");
						sum = sum + Integer.parseInt(No_of_Medals);
					}
					
					if (count == 5) {
						No_of_Medals = tokenizer.nextToken("\t");
						sum = sum + Integer.parseInt(No_of_Medals);
					}
					
					if (count == 5) {
						No_of_Medals = tokenizer.nextToken("\t");
						sum = sum + Integer.parseInt(No_of_Medals);
					}
					
					
					

				}

				// word.set(tokenizer.nextToken());

				// context.write(word, one);
			}
			if (found == 1){
				context.write(new Text(Year), new
				 IntWritable(sum) );
			}
		}
	}

	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum = sum + val.get();
			}
			context.write(key, new IntWritable(sum));
		}
	}

	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = new Job(conf, "Wordcount");
		job.setJarByClass(Assignment_2.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.waitForCompletion(true);
	}
}

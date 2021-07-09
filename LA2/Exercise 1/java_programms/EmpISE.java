import java.io.IOException;
import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;


public class EmpISE {
	//Mapper function
	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
		private final static IntWritable one=new IntWritable(1);
		private Text word=new Text();
		
		public void map(LongWritable key,Text value,OutputCollector<Text,IntWritable> output,Reporter reporter)throws IOException {
			String data=value.toString();
			String[] Ecount=data.split(",");
			if(Ecount[4].equals("ISE")) {
				output.collect(new Text("Total number of employees working in ISE Department : "),one);
			}
		}
	}
	
	//REDUCER function
	public static class Reduce extends MapReduceBase implements Reducer<Text,IntWritable, Text, IntWritable> {
		public void reduce(Text key,Iterator<IntWritable> values,OutputCollector<Text,IntWritable> output,Reporter reporter)throws IOException {
			int val = 0;
			while(values.hasNext()) {
				val+=values.next().get();
			}
			output.collect(key,new IntWritable(val));
		}
	}

	//DRIVER function
	public static void main(String[] args) throws Exception {
		
		JobConf conf=new JobConf(EmpISE.class);
		conf.setJobName("Total number of employees working in ISE Department");
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);
		conf.setMapperClass(Map.class);
		conf.setReducerClass(Reduce.class);
		conf.setCombinerClass(Reduce.class);
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		FileInputFormat.setInputPaths(conf,new Path(args[0]));
		FileOutputFormat.setOutputPath(conf,new Path(args[1]));
		JobClient.runJob(conf);
		

	}

}

package org.conan.myhadoop.mr.kpi;


import java.io.IOException;
import java.text.ParseException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class KPIBytes {

	public static class KPIBytesMapper extends
			Mapper<Object, Text, Text, LongWritable> {		
		private Text word = new Text();
		private LongWritable bytes = new LongWritable();

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			KPI kpi = KPI.filterBytes(value.toString());
			if (kpi.isValid()) {
				try {
					word.set(kpi.getTime_local_DateString());
				} catch (ParseException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				bytes.set(kpi.getSent_bytes());
				context.write(word, bytes);
			}
		}
	}

	public static class KPIBytesReducer extends
			Reducer<Text, LongWritable, Text, LongWritable> {
		private LongWritable result = new LongWritable();

		public void reduce(Text key, Iterable<LongWritable> values,
				Context context) throws IOException, InterruptedException {
			long sum = 0L;
			for (LongWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {
		String input = "hdfs://hd01:9000/log_kpi/logfile";
		String output = "hdfs://hd01:9000/log_kpi/bytes";

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "KPIBytes");
		job.setJarByClass(KPIBytes.class);
		job.setMapperClass(KPIBytesMapper.class);
		job.setCombinerClass(KPIBytesReducer.class);
		job.setReducerClass(KPIBytesReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		FileInputFormat.addInputPath(job, new Path(input));
		FileSystem fs = FileSystem.get(conf);
		Path po = new Path(output);
		if (fs.exists(po)) {
			fs.delete(po, true);
			System.out.println("输出路径"+output+"已存在，成功删除！");
		}
		FileOutputFormat.setOutputPath(job, po);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}

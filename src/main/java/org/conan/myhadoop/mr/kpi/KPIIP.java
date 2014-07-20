package org.conan.myhadoop.mr.kpi;

import java.io.IOException;
import java.text.ParseException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class KPIIP {

	public static class KPIIPMapper extends Mapper<Object, Text, Text, Text> {
		private Text word = new Text();
		private Text ips = new Text();

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			KPI kpi = KPI.filterPVs(value.toString());
			if (kpi.isValid()) {
				try {
					word.set(kpi.getTime_local_DateString());
				} catch (ParseException e) {
					e.printStackTrace();
				}
				ips.set(kpi.getRemote_addr());
				context.write(word, ips);
			}
		}
	}

	public static class KPIIPReducer extends Reducer<Text, Text, Text, Text> {
		private Text result = new Text();		

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			Set<String> ipSet = new HashSet<String>();
			for (Text val : values) {
				ipSet.add(val.toString());
			}
			result.set(String.valueOf(ipSet.size()));
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {
		String input = "hdfs://hd01:9000/log_kpi/logfile";
		String output = "hdfs://hd01:9000/log_kpi/ip";

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "KPIIP");
		job.setJarByClass(org.conan.myhadoop.mr.kpi.KPIIP.class);
		job.setMapperClass(KPIIPMapper.class);

		job.setReducerClass(KPIIPReducer.class);

		// TODO: specify output types
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// TODO: specify input and output DIRECTORIES (not files)
		FileInputFormat.setInputPaths(job, new Path(input));
		FileSystem fs = FileSystem.get(conf);
		Path po = new Path(output);
		if (fs.exists(po)) {
			fs.delete(po, true);
			System.out.println("输出路径" + output + "已存在，成功删除！");
		}
		FileOutputFormat.setOutputPath(job, po);

		if (!job.waitForCompletion(true))
			return;
	}

}

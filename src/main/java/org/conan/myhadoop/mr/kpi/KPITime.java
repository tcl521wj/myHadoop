package org.conan.myhadoop.mr.kpi;


import java.io.IOException;
import java.text.ParseException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class KPITime {

    public static class KPITimeMapper extends Mapper<Object, Text, Text, IntWritable> {
        private IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
            KPI kpi = KPI.filterTime(value.toString());
            if (kpi.isValid()) {
                try {
                    word.set(kpi.getTime_local_Date_hour());
                    context.write(word, one);
                } catch (ParseException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public static class KPITimeReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
    	String input = "hdfs://hd01:9000/log_kpi/logfile";
		String output = "hdfs://hd01:9000/log_kpi/time";

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "KPITime");
		job.setJarByClass(KPITime.class);
		job.setMapperClass(KPITimeMapper.class);
		job.setCombinerClass(KPITimeReducer.class);
		job.setReducerClass(KPITimeReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
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

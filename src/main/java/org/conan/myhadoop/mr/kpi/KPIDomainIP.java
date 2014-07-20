package org.conan.myhadoop.mr.kpi;


import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Reducer;

public class KPIDomainIP {

    public static class KPIDomainIPMapper extends  Mapper<Object, Text, Text, Text> {
        private Text domain = new Text();
        private Text ips = new Text();

        public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
            KPI kpi = KPI.filterDomain(value.toString());
            if (kpi.isValid()) {
                domain.set(kpi.getHttp_referer_domain());
                ips.set(kpi.getRemote_addr());
                context.write(domain, ips);
            }
        }
    }

    public static class KPIDomainIPReducer extends Reducer<Text, Text, Text, Text> {
        private Text result = new Text();        

        public void reduce(Text key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
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
		String output = "hdfs://hd01:9000/log_kpi/domainip";

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "KPIDomainIP");
		job.setJarByClass(KPIDomainIP.class);
		job.setMapperClass(KPIDomainIPMapper.class);
		//job.setCombinerClass(KPIDomainIPReducer.class);
		job.setReducerClass(KPIDomainIPReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.setInputPaths(job, new Path(input));
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

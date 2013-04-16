package org.lab41.sample.etl.mapreduce;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.avro.Schema;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.avro.mapreduce.AvroMultipleOutputs;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.DeflateCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.lab41.sample.etl.domain.SampleRecord;

/**
 * Driver
 * 
 * @author lab41
 * 
 *         The driver that runs and configures the job
 */
public class Driver extends Configured implements Tool {
	private static final SimpleDateFormat directoryFormat = new SimpleDateFormat(
			"_yyyy-MM-dd_HH-mm-ss.SSSS");

	@Override
	public int run(String[] args) throws Exception {
		Path mrInput, mrOutput;
		if (args.length == 2) {
			mrInput = new Path(args[0]);
			mrOutput = new Path(args[1] + directoryFormat.format(new Date()));
		} else {
			System.err.println("Parameter missing!");
			return 1;
		}

		/** configure Job **/
		Job job = new Job(getConf(), "DataIngest Example");
		job.setJarByClass(Driver.class);
		job.setUserClassesTakesPrecedence(true);

		FileInputFormat.setInputPaths(job, mrInput);
		FileOutputFormat.setOutputPath(job, mrOutput);

		job.setMapperClass(MapperRawToAvro.class);
		job.setReducerClass(ReducerByDateTime.class);

		AvroJob.setMapOutputKeySchema(job, Schema.create(Schema.Type.LONG));
		AvroJob.setMapOutputValueSchema(job, SampleRecord.SCHEMA$);

		AvroKeyOutputFormat.setCompressOutput(job, true);
		AvroKeyOutputFormat.setOutputCompressorClass(job, DeflateCodec.class);

		AvroMultipleOutputs.addNamedOutput(job, "sampleRecord",
				AvroKeyOutputFormat.class, SampleRecord.SCHEMA$);
		MultipleOutputs.setCountersEnabled(job, true);

		if (job.waitForCompletion(true)) {
			return 0;
		} else {
			return 1;
		}
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new Driver(), args);

		System.exit(exitCode);
	}
}

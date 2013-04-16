package org.lab41.sample.etl.mapreduce;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroMultipleOutputs;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.lab41.sample.etl.domain.SampleRecord;

/**
 * ReducerByDateTime
 * 
 * @author lab41.org
 * 
 *         This reducer creates and writes an Avro file containing all entries
 *         for a given year
 */

public class ReducerByDateTime extends
		Reducer<AvroKey<Long>, AvroValue<SampleRecord>, Text, NullWritable> {

	private static Log log = LogFactory.getLog(ReducerByDateTime.class);

	private static final SimpleDateFormat directoryFormat = new SimpleDateFormat(
			"'sample-record'/yyyy/");

	private static final SimpleDateFormat fileNameFormat = new SimpleDateFormat(
			"yyyy_'processtime'_'"
					+ Long.toString(System.currentTimeMillis(),
							Character.MAX_RADIX) + "'");
	private AvroMultipleOutputs multipleOutputs;
	private Date outputKeyDate;

	protected String outputDirectory;

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		super.setup(context);

		outputKeyDate = new Date();

		multipleOutputs = new AvroMultipleOutputs(context);
	}

	@Override
	protected void reduce(AvroKey<Long> key,
			Iterable<AvroValue<SampleRecord>> values, Context context)
			throws IOException, InterruptedException {
		outputKeyDate.setTime(key.datum());
		outputDirectory = directoryFormat.format(outputKeyDate);
		// #sample-record/yyyy/yyyy_processtime_IEOXKQ
		String outputFileName = outputDirectory + Path.SEPARATOR
				+ fileNameFormat.format(outputKeyDate);
		log.info("Writing avro file " + outputFileName);

		SampleRecord sampleRecord = null;
		for (AvroValue<SampleRecord> value : values) {
			sampleRecord = value.datum();
			multipleOutputs.write("sampleRecord", new AvroKey<SampleRecord>(
					sampleRecord), NullWritable.get(), outputFileName);
		}
	}

	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		multipleOutputs.close();
		super.cleanup(context);
	}
}

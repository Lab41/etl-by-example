
package org.lab41.sample.etl.mapreduce;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.commons.lang.time.DateUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.lab41.sample.etl.domain.SampleRecord;

/**
 * MapperRawToAvro
 * 
 * @author lab41.org
 * 
 *         A sample mapper class that reads a line from a log file and converts
 *         it to an Avro record, pivotting on the hour since the epoch
 */
public class MapperRawToAvro extends
		Mapper<LongWritable, Text, AvroKey<Long>, AvroValue<SampleRecord>> {

	private static final SimpleDateFormat dateFormat = new SimpleDateFormat(
			"yyyy-MM-dd HH:mm:ss");
	private static final String splitToken = ",";

	private static Log log = LogFactory.getLog(MapperRawToAvro.class);

	private SampleRecord sampleRecord;

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		super.setup(context);
		sampleRecord = new SampleRecord();
	}

	@Override
	public void run(Context context) throws IOException, InterruptedException {
		setup(context);

		try {
			while (context.nextKeyValue()) {
				map(context.getCurrentKey(), context.getCurrentValue(), context);
			}
		} catch (IOException ex) {
			log.error("Exception caught in the mapper.", ex);
		}
	}

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		Date startTime = null;

		String[] record = value.toString().split(splitToken, -1);

		try {

			startTime = dateFormat.parse(record[4]);

			sampleRecord.setRequiredName(record[0]);
			sampleRecord.setOptionalName(record[1]);
			sampleRecord.setDataItemLong(Long.parseLong(record[2]));
			sampleRecord.setDataItemInt(Integer.parseInt(record[3]));
			sampleRecord.setStartTime(startTime.getTime());
			sampleRecord.setEndTime(dateFormat.parse(record[5]).getTime());

		} catch (ParseException e) {
			log.error("Parse Error: " + value.toString());
			e.printStackTrace();
		}

		/**
		 * Pivot on the milliseconds since the the epoch rounded down to nearest hour
		 */
		AvroKey<Long> avroKey = new AvroKey<Long>(DateUtils.truncate(startTime,
				Calendar.YEAR).getTime());
		AvroValue<SampleRecord> avroValue = new AvroValue<SampleRecord>(
				sampleRecord);

		context.write(avroKey, avroValue);

	}

}

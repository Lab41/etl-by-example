package org.lab41.sample.etl.mapreduce;

import static org.junit.Assert.*;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

import org.apache.avro.Schema;

import org.apache.avro.mapred.AvroKey;
import org.apache.avro.hadoop.io.AvroSerialization;
import org.apache.avro.mapred.AvroValue;
import org.apache.commons.lang.time.DateUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.lab41.sample.etl.domain.SampleRecord;
import org.lab41.sample.etl.mapreduce.MapperRawToAvro;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * RawToAvroTest
 * 
 * @author lab41
 * 
 *         Tests the mapper
 */
public class RawToAvroTest {

	Logger logger = LoggerFactory.getLogger(RawToAvroTest.class);
	private MapDriver<LongWritable, Text, AvroKey<Long>, AvroValue<SampleRecord>> mapDriver;
	SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	@BeforeClass
	public static void setUpClass() {
	}

	@AfterClass
	public static void tearDownClass() {
	}

	@Before
	public void setUp() throws IOException {

		MapperRawToAvro mapper = new MapperRawToAvro();

		mapDriver = new MapDriver<LongWritable, Text, AvroKey<Long>, AvroValue<SampleRecord>>();
		mapDriver.setMapper(mapper);
		String[] strings = mapDriver.getConfiguration().getStrings(
				"io.serializations");

		String[] newStrings = new String[strings.length + 1];
		System.arraycopy(strings, 0, newStrings, 0, strings.length);
		newStrings[newStrings.length - 1] = AvroSerialization.class.getName();

		mapDriver.getConfiguration()
				.setStrings("io.serializations", newStrings);
		mapDriver.getConfiguration().setStrings(
				"avro.serialization.key.writer.schema",
				Schema.create(Schema.Type.LONG).toString(true));
		mapDriver.getConfiguration().setStrings(
				"avro.serialization.value.writer.schema",
				SampleRecord.SCHEMA$.toString(true));

	}

	@After
	public void tearDown() {
	}

	@Test
	public void testMap0() throws ParseException {

		String test0 = "PrimaryName,OptionalName,128918981,3232,2004-02-12 14:08:44,2004-02-12 14:09:47";

		Text testInputText = new Text(test0);

		SampleRecord record = new SampleRecord();

		record.setRequiredName("PrimaryName");
		record.setOptionalName("OptionalName");
		record.setDataItemLong(128918981L);
		record.setDataItemInt(3232);
		record.setStartTime(sdf.parse("2004-02-12 14:08:44").getTime());
		record.setEndTime(sdf.parse("2004-02-12 14:09:47").getTime());

		System.out.println("TIME: "
				+ DateUtils.truncate(sdf.parse("1980-02-01 04:04:14"),
						Calendar.YEAR).getTime());

		AvroKey<Long> expectedPivot = new AvroKey<Long>(1072944000000L);
		AvroValue<SampleRecord> expectedRecord = new AvroValue<SampleRecord>(
				record);

		assertNotNull(expectedPivot);
		assertNotNull(testInputText);

		mapDriver.withInput(new LongWritable(1L), testInputText);
		mapDriver.withOutput(new Pair<AvroKey<Long>, AvroValue<SampleRecord>>(
				expectedPivot, expectedRecord));
		mapDriver.runTest();
	}

	@Test
	public void testMap1() throws ParseException {
		String test1 = "Name,,128918981,3232,1980-02-01 04:04:14,2002-02-12 14:09:47";

		Text testInputText = new Text(test1);

		SampleRecord record = new SampleRecord();

		record.setRequiredName("Name");
		record.setOptionalName("");
		record.setDataItemLong(128918981L);
		record.setDataItemInt(3232);
		record.setStartTime(sdf.parse("1980-02-01 04:04:14").getTime());
		record.setEndTime(sdf.parse("2002-02-12 14:09:47").getTime());

		AvroKey<Long> expectedPivot = new AvroKey<Long>(315561600000L);
		AvroValue<SampleRecord> expectedRecord = new AvroValue<SampleRecord>(
				record);

		assertNotNull(expectedPivot);
		assertNotNull(testInputText);

		mapDriver.withInput(new LongWritable(1L), testInputText);
		mapDriver.withOutput(new Pair<AvroKey<Long>, AvroValue<SampleRecord>>(
				expectedPivot, expectedRecord));
		mapDriver.runTest();

	}
}

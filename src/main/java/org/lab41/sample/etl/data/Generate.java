package org.lab41.sample.etl.data;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Generate
 * 
 * @author lab41
 * 
 *         This class generates sample log data
 */
public class Generate implements Runnable {

	private final int lineAverage;
	private final String outputDir;

	public Generate(String outputDir, int lineAverage) {

		this.outputDir = outputDir;
		this.lineAverage = lineAverage;

	}

	private static void printUsage() {
		System.out
				.println("Usage: Generate [outputDir] [numFiles] [threadPoolSize] [averageLines]\n "
						+ "\toutputDir: The directory to output the log files\n"
						+ "\tnumFiles: The number of log files to create\n"
						+ "\tthreadPoolSize: The number of concurrent threads to create files\n"
						+ "\taverageLines: The average lines per log file");
	}

	public static void main(String[] args) {

		int numWorkers = 1;
		int threadPoolSize = 1;
		int lineAverage = 1;
		String outputDir;

		if (args.length != 4) {
			printUsage();
			System.exit(0);
		}

		outputDir = args[0];

		File f = new File(outputDir);
		if (!f.exists()) {

			String currentDir = new File(".").getAbsolutePath();
			System.err.println("Error: Output dir \"" + outputDir
					+ "\" does not exist in " + currentDir);
			System.exit(1);
		}

		numWorkers = Integer.parseInt(args[1]);
		threadPoolSize = Integer.parseInt(args[2]);
		lineAverage = Integer.parseInt(args[3]);

		ExecutorService tpes = Executors.newFixedThreadPool(threadPoolSize);
		Generate[] workers = new Generate[numWorkers];
		for (int i = 0; i < numWorkers; i++) {
			try {
				Thread.sleep(50);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			workers[i] = new Generate(outputDir, lineAverage);
			tpes.execute(workers[i]);
		}
		tpes.shutdown();
	}

	public void run() {

		long time = new Date().getTime();
		Random random = new Random(time);

		int lines = random.nextInt(this.lineAverage);

		System.out.println("Writing file with " + lines + " lines");

		String filename = "out-" + time;
		try {
			writeFile(outputDir, filename, lines, random);
			System.out.println("File written: " + filename);

		} catch (IOException e) {
			System.err.println("Error file creation");
			e.printStackTrace();
		}

	}

	private void writeFile(String parentDir, String filename, int lines,
			Random random) throws IOException {
		BufferedWriter writer = new BufferedWriter(new FileWriter(new File(
				outputDir + "/" + filename), true));

		int writeZero = random.nextInt(10);

		if (writeZero > 0) {
			for (int i = 0; i < lines; i++) {
				writer.write(getRandomLogEntry(random));
				writer.newLine();
				writer.flush();
			}
		}
		writer.close();
	}

	String splitToken = ",";

	private String getRandomLogEntry(Random random) {

		int randomInt = random.nextInt();
		long randomLong = random.nextLong();

		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

		Date start = getRandomDate(random);
		Date end = new Date(start.getTime() + random.nextInt(65535));

		String startTime = format.format(start);
		String endTime = format.format(end);

		String primary = UUID.randomUUID().toString().substring(0, 20);
		String optional = "";

		// sometimes we want the optional name
		if (random.nextInt() > 4444000) {
			optional = UUID.randomUUID().toString().substring(0, 20);
		}
		return primary + splitToken + optional + splitToken + randomLong
				+ splitToken + randomInt + splitToken + startTime + splitToken
				+ endTime;
	}

	private Date getRandomDate(Random random) {
		Calendar calendar = Calendar.getInstance();

		calendar.set(Calendar.YEAR, 1972 + random.nextInt(38));
		calendar.set(Calendar.DAY_OF_MONTH, random.nextInt(31) + 1);
		calendar.set(Calendar.HOUR_OF_DAY, random.nextInt(24));
		calendar.set(Calendar.MINUTE, random.nextInt(60));
		calendar.set(Calendar.SECOND, random.nextInt(60));

		return calendar.getTime();
	}

}

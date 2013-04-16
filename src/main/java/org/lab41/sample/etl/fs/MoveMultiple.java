package org.lab41.sample.etl.fs;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

/**
 * MoveMultiple
 * 
 * @author lab41
 * 
 *         Until Oozie supports moving multiple files, this class should do the
 *         trick
 */
public class MoveMultiple {
	private static final String OOZIE_ACTION_OUTPUT_PROPERTIES = "oozie.action.output.properties";

	public static void main(String[] args) throws Exception {
		String inputPath = args[0];
		String outputPath = args[1];

		String propKey0 = "status";

		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		Path hadoopInputDir = new Path(inputPath);
		Path hadoopOutputDir = new Path(outputPath);

		if (!fs.exists(hadoopInputDir)) {
			throw new RuntimeException(" Error: " + inputPath
					+ " does not exist");
		}

		if (!fs.exists(hadoopOutputDir)) {
			throw new RuntimeException("Error: " + outputPath
					+ " does not exist");
		}

		RemoteIterator<LocatedFileStatus> itr = fs.listFiles(hadoopInputDir,
				true);
		LocatedFileStatus fileStatus;

		int numFiles = 0;

		int workingPathLen = fs.getFileStatus(hadoopInputDir).getPath()
				.toString().length() + 1; // +1 for trailing slash

		String relativePath;

		while (itr.hasNext()) {
			fileStatus = itr.next();

			relativePath = fileStatus.getPath().toString()
					.substring(workingPathLen);

			Path specificFinalOutput = new Path(hadoopOutputDir
					+ Path.SEPARATOR + relativePath);

			/**
			 * if the parent directory does not exist in the destination, then
			 * create it
			 */
			if (!fs.isDirectory(specificFinalOutput.getParent())) {
				fs.mkdirs(specificFinalOutput.getParent());
			}

			numFiles++;

			System.out.println("Moving " + fileStatus.getPath() + " to "
					+ specificFinalOutput);
			fs.rename(fileStatus.getPath(), specificFinalOutput);

		}

		if (numFiles == 0) {
			System.out.println("no files..........");
			throw new RuntimeException("No Files to process");
		}

		
		/**
		 * Let's let oozie know the number of files that were copied
		 */
		String oozieProp = System.getProperty(OOZIE_ACTION_OUTPUT_PROPERTIES);
		if (oozieProp != null) {

			File propFile = new File(oozieProp);
			Properties props = new Properties();
			props.setProperty(propKey0, "Success " + numFiles + " copied");
			OutputStream os = new FileOutputStream(propFile);
			props.store(os, "");
			os.close();
		} else {
			System.err
					.println("Not using oozie since properties file does not exits");

			throw new Exception();
		}
	}

}
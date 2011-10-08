/*
 * Mavuno: A Hadoop-Based Text Mining Toolkit
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You may
 * obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0 
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package edu.isi.mavuno.util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.log4j.Logger;

/**
 * @author metzler
 *
 */
public class MavunoUtils {

	private static final Logger sLogger = Logger.getLogger(MavunoUtils.class);

	public static final Text SPACE = new Text(" ");
	public static final byte [] SPACE_BYTES = SPACE.getBytes();
	public static final int SPACE_BYTES_LENGTH = SPACE.getLength();

	public static final Text TAB = new Text("\t");
	public static final byte [] TAB_BYTES = TAB.getBytes();
	public static final int TAB_BYTES_LENGTH = TAB.getLength();

	public static final Text PIPE = new Text("|");
	public static final byte [] PIPE_BYTES = PIPE.getBytes();
	public static final int PIPE_BYTES_LENGTH = PIPE.getLength();

	public static final Text ASTERISK = new Text("*");
	public static final byte [] ASTERISK_BYTES = ASTERISK.getBytes();
	public static final int ASTERISK_BYTES_LENGTH = ASTERISK.getLength();

	// read parameters from local filesystem
	public static void readParameters(String [] args, String prefix, Configuration conf) throws IOException {
		FileSystem fs = FileSystem.getLocal(conf);
		for(String arg : args) {
			if(arg.startsWith("-")) {
				int equalsIndex = arg.indexOf('=');
				if(equalsIndex == -1) {
					sLogger.warn("Ignoring malformed parameter -- " + arg);
				}
				String paramName = arg.substring(1, equalsIndex);
				String paramValue = arg.substring(equalsIndex+1, arg.length());
				conf.set(prefix + "." + paramName, paramValue);
			}
			else {
				// open parameter file
				BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(new Path(arg))));

				// read/set parameters
				String input;
				while((input = reader.readLine()) != null) {
					String [] cols = input.split("\t");
					if(cols.length != 2) {
						sLogger.warn("Skipping malformed parameter file line -- " + input);
					}
					else {
						conf.set(prefix + "." + cols[0], cols[1]);
					}
				}

				reader.close();
			}
		}
	}

	public static void recursivelyAddInputPaths(Job job, String path) throws IOException {
		FileSystem fs;
		try {
			fs = FileSystem.get(new URI(path), job.getConfiguration());
		} catch (URISyntaxException e) {
			throw new RuntimeException("Error recursively adding path -- " + path);
		}

		FileStatus [] ls = fs.listStatus(new Path(path));
		for(FileStatus status : ls) {
			// skip anything that starts with an underscore, as it often indicates
			// a log directory or another special type of Hadoop file
			if(status.getPath().getName().startsWith("_")) {
				continue;
			}

			if(status.isDir()) {
				recursivelyAddInputPaths(job, status.getPath().toString());
			}
			else {
				FileInputFormat.addInputPath(job, status.getPath());
			}
		}
	}

	public static void createDirectory(Configuration conf, String path) throws IOException {
		FileSystem fs;
		try {
			fs = FileSystem.get(new URI(path), conf);
		} catch(URISyntaxException e) {
			throw new RuntimeException("Error creating directory -- " + path);
		}
		fs.mkdirs(new Path(path));
	}

	public static void removeDirectory(Configuration conf, String path) throws IOException {
		FileSystem fs;
		try {
			fs = FileSystem.get(new URI(path), conf);
		} catch(URISyntaxException e) {
			throw new RuntimeException("Error removing directory -- " + path);
		}
		fs.delete(new Path(path), true);
	}

	public static FileStatus [] getDirectoryListing(Configuration conf, String path) throws IOException {
		FileSystem fs;
		try {
			fs = FileSystem.get(new URI(path), conf);
		} catch(URISyntaxException e) {
			throw new RuntimeException("Error getting directory listing -- " + path);
		}
		FileStatus [] files = fs.listStatus(new Path(path));
		return files;
	}

	public static FSDataInputStream getFSDataInputStream(Configuration conf, String path) throws IOException {
		FileSystem fs;
		try {
			fs = FileSystem.get(new URI(path), conf);
		} catch(URISyntaxException e) {
			throw new RuntimeException("Error opening file -- " + path);
		}		
		return fs.open(new Path(path));
	}

	public static BufferedReader getBufferedReader(Configuration conf, String path) throws IOException {
		FileSystem fs;
		try {
			fs = FileSystem.get(new URI(path), conf);
		} catch(URISyntaxException e) {
			throw new RuntimeException("Error opening file -- " + path);
		}		
		return new BufferedReader(new InputStreamReader(fs.open(new Path(path))));
	}

	public static BufferedWriter getBufferedWriter(Configuration conf, String path) throws IOException {
		FileSystem fs;
		try {
			fs = FileSystem.get(new URI(path), conf);
		} catch(URISyntaxException e) {
			throw new RuntimeException("Error creating file -- " + path);
		}		
		return new BufferedWriter(new OutputStreamWriter(fs.create(new Path(path)), "UTF8"));
	}

	public static boolean pathExists(Configuration conf, String path) throws IOException {
		try {
			return FileSystem.get(new URI(path), conf).exists(new Path(path));
		} catch (URISyntaxException e) {
			throw new RuntimeException("Error checking if path exists -- " + path);			
		}
	}
	public static String getRequiredParam(String param, Configuration conf) {
		String result = conf.get(param, null);
		if(result == null) {
			throw new RuntimeException(param + " not specified!");
		}
		return result;
	}

	public static String getOptionalParam(String param, Configuration conf) {
		String result = conf.get(param, null);
		return result;
	}

	public static long getTotalTerms(Configuration conf, String totalTermsPath) throws IOException {
		BufferedReader reader = getBufferedReader(conf, totalTermsPath);
		long totalTerms = Long.parseLong(reader.readLine());
		reader.close();

		return totalTerms;
	}

	// constructs a (pipe-delimted) context given an arbitrary number of Text objects
	public static final Text createContext(Text ... args) {
		return createContext("|", args);
	}

	// constructs a separator delimited context given an arbitrary number of Text objects
	public static final Text createContext(String separator, Text ... args) {
		Text context = new Text();

		for(int i = 0; i < args.length; i++) {
			context.append(args[i].getBytes(), 0, args[i].getLength());
			if(i != args.length - 1) {
				context.append(separator.getBytes(), 0, separator.length());
			}
		}
		
		return context;
	}
		
	// a proper modulus operator
	public static final int mod(int x, int y)
	{
		int result = x % y;
		if (result < 0) {
			result += y;
		}
		return result;
	}
	
	public static final byte[] intToByteArray(int value) {
		return ByteBuffer.allocate(4).putInt(value).array();
	}
}

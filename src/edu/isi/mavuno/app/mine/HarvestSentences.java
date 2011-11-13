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

package edu.isi.mavuno.app.mine;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

import opennlp.tools.sentdetect.SentenceDetector;
import opennlp.tools.sentdetect.SentenceDetectorME;
import opennlp.tools.sentdetect.SentenceModel;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import edu.isi.mavuno.input.Indexable;
import edu.isi.mavuno.input.SentenceSegmentedDocument;
import edu.isi.mavuno.nlp.NLProcTools;
import edu.isi.mavuno.util.MavunoUtils;
import edu.isi.mavuno.util.SentenceWritable;

/**
 * @author metzler
 *
 */
public class HarvestSentences extends Configured implements Tool {
	private static final Logger sLogger = Logger.getLogger(HarvestSentences.class);

	public HarvestSentences(Configuration conf) {
		super(conf);
	}

	private static class MyMapper extends Mapper<Writable, Indexable, Text, Text> {

		private static final Text mKey = new Text();
		private static final Text mValue = new Text();

		private Set<Pattern> mPatterns = null;
		
		private SentenceDetector mSentenceDetector = null;
		
		@Override
		public void setup(Mapper<Writable, Indexable, Text, Text>.Context context) throws IOException {
			Configuration conf = context.getConfiguration();
			try {
				// read patterns
				mPatterns = new HashSet<Pattern>();
				
				String patternPath = conf.get("Mavuno.HarvestSentences.PatternPath", null);
				BufferedReader reader = MavunoUtils.getBufferedReader(conf, patternPath);
				
				String input;
				while((input = reader.readLine()) != null) {
					mPatterns.add(Pattern.compile(input));
				}
				
				reader.close();
				
				// initialize sentence detector
				mSentenceDetector = new SentenceDetectorME(new SentenceModel(getClass().getClassLoader().getResourceAsStream(NLProcTools.DEFAULT_SENTENCE_MODEL)));
			}
			catch(Exception e) {
				throw new RuntimeException(e);
			}
		}

		@SuppressWarnings({ "unchecked", "rawtypes" })
		@Override
		public void map(Writable key, Indexable doc, Mapper<Writable, Indexable, Text, Text>.Context context) throws IOException, InterruptedException {
			
			List<String> sentences = new ArrayList<String>();

			if(doc instanceof SentenceSegmentedDocument) {
				List<SentenceWritable> segmentedSentences = ((SentenceSegmentedDocument)doc).getSentences();
				for(SentenceWritable sentence : segmentedSentences) {
					sentences.add(sentence.toString());
				}
			}
			else {
				sentences = Arrays.asList(mSentenceDetector.sentDetect(doc.getContent().replace('\n', ' ')));
			}

			for(String sentence : sentences) {
				for(Pattern p : mPatterns) {
					mKey.set(p.pattern());
					if(p.matcher(sentence).find()) {
						mValue.set(sentence);
						context.write(mKey, mValue);
					}
				}
			}
		}
		
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.util.Tool#run(java.lang.String[])
	 */
	@Override
	public int run(String[] args) throws ClassNotFoundException, InterruptedException, IOException {
		MavunoUtils.readParameters(args, "Mavuno.HarvestSentences", getConf());
		return run();
	}
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public int run() throws ClassNotFoundException, InterruptedException, IOException {
		Configuration conf = getConf();

		String patternPath = MavunoUtils.getRequiredParam("Mavuno.HarvestSentences.PatternPath", conf);
		String corpusPath = MavunoUtils.getRequiredParam("Mavuno.HarvestSentences.CorpusPath", conf);
		String corpusClass = MavunoUtils.getRequiredParam("Mavuno.HarvestSentences.CorpusClass", conf);
		String outputPath = MavunoUtils.getRequiredParam("Mavuno.HarvestSentences.OutputPath", conf);

		sLogger.info("Tool name: HarvestSentences");
		sLogger.info(" - Pattern file: " + patternPath);
		sLogger.info(" - Corpus path: " + corpusPath);
		sLogger.info(" - Corpus class: " + corpusClass);
		sLogger.info(" - Output path: " + outputPath);
		
		Job job = new Job(conf);
		job.setJobName("HarvestSentences");

		MavunoUtils.recursivelyAddInputPaths(job, corpusPath);
		FileOutputFormat.setOutputPath(job, new Path(outputPath));

		job.setInputFormatClass((Class<? extends InputFormat>)Class.forName(corpusClass));
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setMapperClass(MyMapper.class);
		job.setNumReduceTasks(0);

		job.waitForCompletion(true);		
		
		return 0;
	}

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		int res = ToolRunner.run(new HarvestSentences(conf), args);
		System.exit(res);
	}

}

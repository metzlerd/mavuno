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

package edu.isi.mavuno.app.nlp;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import edu.isi.mavuno.input.Indexable;
import edu.isi.mavuno.input.TratzParsedDocument;
import edu.isi.mavuno.util.MavunoUtils;
import edu.isi.mavuno.util.SentenceWritable;
import edu.isi.mavuno.util.TratzParsedTokenWritable;

/**
 * @author metzler
 *
 */
public class HarvestParseGraph extends Configured implements Tool {
	private static final Logger sLogger = Logger.getLogger(HarvestParseGraph.class);

	public HarvestParseGraph(Configuration conf) {
		super(conf);
	}

	private static class MyMapper extends Mapper<Writable, Indexable, Text, LongWritable> {

		private final Text mKey = new Text();
		private final LongWritable mValue = new LongWritable(1);

		@Override
		public void map(Writable key, Indexable doc, Mapper<Writable, Indexable, Text, LongWritable>.Context context) throws IOException, InterruptedException {
			if(doc instanceof TratzParsedDocument) {
				TratzParsedDocument d = (TratzParsedDocument)doc;
				List<SentenceWritable<TratzParsedTokenWritable>> sentences = d.getSentences();
				for(SentenceWritable<TratzParsedTokenWritable> s : sentences) {
					List<TratzParsedTokenWritable> tokens = s.getTokens();

					for(int index = 0; index < tokens.size(); index++) {
						TratzParsedTokenWritable t = tokens.get(index);

						String term = t.getToken().toString().toLowerCase();
						Text posTag = t.getPosTag();
						int dependIndex = t.getDependIndex();
						String dependType = t.getDependType().toString();

						if(dependIndex != 0) {
							String dependText = tokens.get(dependIndex-1).getToken().toString().toLowerCase();
							String dependPosTag = tokens.get(dependIndex-1).getPosTag().toString();
							
							if(dependPosTag.equals("CC") || dependPosTag.equals("IN") || dependPosTag.equals("TO")) {
								dependIndex = tokens.get(dependIndex-1).getDependIndex();
								dependType = dependType + "_" + dependPosTag;

								if(dependIndex != 0) {
									dependText = tokens.get(dependIndex-1).getToken().toString().toLowerCase();
									dependPosTag = tokens.get(dependIndex-1).getPosTag().toString();
								}
								else {
									dependText = "[**ROOT**]";
									dependPosTag = "[**ROOT**]";
								}
							}
							
							mKey.set(term + "\t" + dependText + "\t" + posTag + "\t" + dependPosTag + "\t" + dependType);
						}
						else {
							mKey.set(term + "\t" + "[**ROOT**]" + "\t" + posTag + "\t" + "[**ROOT**]" + "\t" + dependType);
						}

						context.write(mKey, mValue);
					}
				}
			}
			else {
				throw new RuntimeException("HarvestParseGraph only applicable to TratzParsedDocument inputs!");
			}
		}
	}

	private static class MyReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

		private final LongWritable mValue = new LongWritable();
		
		@Override
		public void reduce(Text key, Iterable<LongWritable> values, Reducer<Text, LongWritable, Text, LongWritable>.Context context) throws IOException, InterruptedException {
			long sum = 0;
			for(LongWritable c : values) {
				sum += c.get();
			}
			mValue.set(sum);
			context.write(key, mValue);
		}

	}
	
	/* (non-Javadoc)
	 * @see org.apache.hadoop.util.Tool#run(java.lang.String[])
	 */
	@Override
	public int run(String[] args) throws ClassNotFoundException, InterruptedException, IOException {
		MavunoUtils.readParameters(args, "Mavuno.HarvestParseGraph", getConf());
		return run();
	}
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public int run() throws ClassNotFoundException, InterruptedException, IOException {
		Configuration conf = getConf();

		String corpusPath = MavunoUtils.getRequiredParam("Mavuno.HarvestParseGraph.CorpusPath", conf);
		String corpusClass = MavunoUtils.getRequiredParam("Mavuno.HarvestParseGraph.CorpusClass", conf);
		String outputPath = MavunoUtils.getRequiredParam("Mavuno.HarvestParseGraph.OutputPath", conf);

		sLogger.info("Tool name: HarvestParseGraph");
		sLogger.info(" - Corpus path: " + corpusPath);
		sLogger.info(" - Corpus class: " + corpusClass);
		sLogger.info(" - Output path: " + outputPath);

		Job job = new Job(conf);
		job.setJobName("HarvestParseGraph");

		MavunoUtils.recursivelyAddInputPaths(job, corpusPath);
		FileOutputFormat.setOutputPath(job, new Path(outputPath));

		job.setInputFormatClass((Class<? extends InputFormat>)Class.forName(corpusClass));
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);

		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);

		job.waitForCompletion(true);		

		return 0;
	}


	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		int res = ToolRunner.run(new HarvestParseGraph(conf), args);
		System.exit(res);
	}

}

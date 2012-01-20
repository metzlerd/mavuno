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

package edu.isi.mavuno.score;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;

import edu.isi.mavuno.util.ScoreWritable;
import edu.isi.mavuno.util.ContextPatternStatsWritable;
import edu.isi.mavuno.util.ContextPatternWritable;

/**
 * @author metzler
 *
 */
public class TFIDFScorer extends Scorer {

	private static double K1 = 1.75;
	
	private enum TF { BINARY, TF, LOG, OKAPI };
	private enum IDF { NONE, LOG, OKAPI };
	
	private TF mTfType;
	private IDF mIdfType;
	
	public TFIDFScorer() {
		super();
	}

	@Override
	public void setup(Configuration conf) throws IOException {
		String paramSpec = conf.get("Mavuno.Scorer.Args", null);
		if(paramSpec != null) {
			String [] params = paramSpec.split(":");
			if(params.length == 2) {
				
				// get tf type
				String tfType = params[0].toLowerCase().trim();
				if("binary".equals(tfType)) {
					mTfType = TF.BINARY;
				}
				else if("tf".equals(tfType)) {
					mTfType = TF.BINARY;
				}
				else if("log".equals(tfType)) {
					mTfType = TF.LOG;
				}
				else if("okapi".equals(tfType)) {
					mTfType = TF.OKAPI;
				}
				else {
					throw new RuntimeException("Invalid tf type -- " + tfType);
				}
				
				// get idf type
				String idfType = params[1].toLowerCase().trim();
				if("none".equals(idfType)) {
					mIdfType = IDF.NONE;
				}
				else if("idf".equals(idfType)) {
					mIdfType = IDF.LOG;
				}
				else if("okapi".equals(idfType)) {
					mIdfType = IDF.OKAPI;
				}
				else {
					throw new RuntimeException("Invalid idf type -- " + tfType);
				}
			}
			else {
				throw new RuntimeException("Invalid TFIDFScorer arguments --" + Arrays.toString(params));
			}
		}
	}
	
	@Override
	public void scorePattern(ContextPatternWritable context, ContextPatternStatsWritable stats, ScoreWritable result) {
		double tf = getTF(stats.matches);
		double idf = getIDF(stats.globalPatternCount, getTotalTerms());
		
		result.score = stats.weight * tf * idf;
		result.staticScore = 0.0;
		result.norm = 0.0;
	}

	@Override
	public void scoreContext(ContextPatternWritable context, ContextPatternStatsWritable stats, ScoreWritable result) {
		double tf = getTF(stats.matches);
		double idf = getIDF(stats.globalContextCount, getTotalTerms());

		result.score = stats.weight * tf * idf;
		result.staticScore = 0.0;
		result.norm = 0.0;
	}

	@Override
	public double finalizeScore(double matchScore, double staticScore, double norm, int numPatterns, double totalPatternWeight) {
		return matchScore / totalPatternWeight;
	}
	
	private final double getTF(long matches) {
		double tf = 0.0;
		
		if(mTfType == TF.BINARY) {
			tf = (matches > 0) ? 1 : 0;
		}
		else if(mTfType == TF.TF) {
			tf = matches;
		}
		else if(mTfType == TF.LOG) {
			tf = Math.log(matches + 1.0);
		}
		else if(mTfType == TF.OKAPI) {
			tf = matches * (K1 + 1.0) / (matches + K1);
		}
		
		return tf;
	}
	
	private final double getIDF(long cf, long colLen) {
		double idf = 0.0;
		
		if(mIdfType == IDF.NONE) {
			idf = 1.0;
		}
		else if(mIdfType == IDF.LOG) {
			idf = Math.log((double)colLen / (double)cf);
		}
		else if(mIdfType == IDF.OKAPI) {
			idf = Math.log(((double)colLen - (double)cf + 0.5) / (cf + 0.5));			
		}
		
		return idf;
	}
}

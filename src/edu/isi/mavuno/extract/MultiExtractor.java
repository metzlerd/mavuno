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

package edu.isi.mavuno.extract;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

import edu.isi.mavuno.input.Indexable;
import edu.isi.mavuno.util.ContextPatternWritable;

/**
 * @author metzler
 *
 */
public class MultiExtractor extends Extractor {

	private Extractor [] mExtractors = null;
	private int mCurExtractor;
	
	public MultiExtractor() {
	}

	@Override
	public void initialize(String argSpec, Configuration conf) throws IOException {
		// parse colon-delimited options
		String [] extractorSpecs = argSpec.split("\\|");
		
		mExtractors = new Extractor[extractorSpecs.length/2];
		for(int i = 0; i < extractorSpecs.length; i += 2) {
			try {
				mExtractors[i/2] = (Extractor)Class.forName(extractorSpecs[i]).newInstance();
				mExtractors[i/2].initialize(extractorSpecs[i+1], conf);
			}
			catch(Exception e) {
				throw new RuntimeException("Error initializing MultiExtractor! -- " + e);
			}
		}
		
		mCurExtractor = 0;
	}

	@Override
	public void setDocument(Indexable doc) {
		for(int i = 0; i < mExtractors.length; i++) {
			mExtractors[i].setDocument(doc);
		}
		mCurExtractor = 0;
	}

	@Override
	public boolean getNextPair(ContextPatternWritable pair) {
		// get the next pair from the current extractor
		boolean hasNext = mExtractors[mCurExtractor].getNextPair(pair);

		if(hasNext) {
			return true;
		}
		else { // advance extractor
			while(!hasNext) {
				mCurExtractor++;
				if(mCurExtractor < mExtractors.length) {
					hasNext = mExtractors[mCurExtractor].getNextPair(pair);
				}
				else {
					break;
				}
			}
			if(hasNext) {
				return true;
			}
			else {
				return false;
			}
		}
	}
}

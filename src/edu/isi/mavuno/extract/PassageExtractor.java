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
import org.apache.hadoop.io.Text;

import edu.isi.mavuno.input.Indexable;
import edu.isi.mavuno.input.Passagifiable;
import edu.isi.mavuno.util.ContextPatternWritable;
import edu.isi.mavuno.util.MavunoUtils;

public class PassageExtractor extends Extractor {

	// sub-extractor
	private Extractor mSubExtractor = null;

	// current document
	private Passagifiable mDoc = null;

	// current document id
	private final Text mDocId = new Text();

	// current passage number and id
	private int mPassageOffset;
	private Text mPassageId = new Text();

	// passage specification
	private int mPassageSize;
	private int mPassageIncrement;

	@Override
	public void initialize(String argSpec, Configuration conf) throws IOException {
		// parse colon-delimited options
		String [] args = argSpec.split(";");

		// initialize sub-extractor
		try {
			mSubExtractor = (Extractor)Class.forName(args[0]).newInstance();
			mSubExtractor.initialize(args[1], conf);
		} 
		catch (Exception e) {
			throw new RuntimeException("Error instantiating PassageExtractor sub-extractor! -- " + e);
		}

		mPassageSize = Integer.parseInt(args[2]);
		mPassageIncrement = Integer.parseInt(args[3]);
	}

	@Override
	public void setDocument(Indexable doc) {
		if(doc instanceof Passagifiable) {
			// set current document
			mDoc = (Passagifiable)doc;

			// set docid
			mDocId.set(doc.getDocid());

			// set passage number and id
			mPassageOffset = 0;
			mPassageId.set(Integer.toString(mPassageOffset));
			
			// set up initial passage
			setupNextPassage();
		}
		else {
			throw new RuntimeException("PassageExtractor only applicable to Passagifiable inputs!");
		}
	}

	@Override
	public boolean getNextPair(ContextPatternWritable pair) {
		// sub-extractor has more passages to process
		if(mSubExtractor != null && mSubExtractor.getNextPair(pair)) {
			pair.setContext(MavunoUtils.createContext(mDocId, mPassageId));
			return true;
		}

		// sub-extractor out of passages
		if(!setupNextPassage()) {
			return false;
		}

		// try to extract a pair from the new passage
		if(mSubExtractor.getNextPair(pair)) {
			pair.setContext(MavunoUtils.createContext(mDocId, mPassageId));
			return true;				
		}

		return false;
	}

	private boolean setupNextPassage() {
		if(mDoc == null) {
			return false;
		}

		// get next passage
		Indexable passage = mDoc.getPassage(mPassageOffset, mPassageSize);

		// if there are no more passages in the document then we're done
		if(passage == null) {
			return false;
		}

		// otherwise, set up next passage
		mSubExtractor.setDocument(passage);

		// update passage offset and id
		mPassageOffset += mPassageIncrement;
		mPassageId.set(Integer.toString(mPassageOffset));

		return true;
	}

}

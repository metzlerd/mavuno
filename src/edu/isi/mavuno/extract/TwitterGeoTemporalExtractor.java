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
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Iterator;
import java.util.List;
import java.util.TimeZone;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;

import edu.isi.mavuno.input.Indexable;
import edu.isi.mavuno.input.TwitterDocument;
import edu.isi.mavuno.util.ContextPatternWritable;
import edu.isi.mavuno.util.MavunoUtils;

/**
 * @author metzler
 *
 */
public class TwitterGeoTemporalExtractor extends Extractor {

	// calendar object (uses GMT locale)
	private final Calendar mCalendar = Calendar.getInstance(TimeZone.getTimeZone("GMT+0000"));

	// maximum n-gram size
	private int mMaxGramSize;

	// temporal granularity
	private String mTemporalGranularity;
	
	// left temporal window size
	private int mLeftTimeWindow;

	// right temporal window size
	private int mRightTimeWindow;
	
	// whether to use geo and/or temporal information as the context
	private boolean mGeo;
	private boolean mTemporal;

	private final Text mPattern = new Text();
	private final Text mLocation = new Text();

	private Iterator<ContextPatternWritable> mGeoTemporalPairsIter = null;
	private final List<ContextPatternWritable> mGeoTemporalPairs = new ArrayList<ContextPatternWritable>();

	/* (non-Javadoc)
	 * @see edu.isi.mavuno.extract.Extractor#initialize(java.lang.String, org.apache.hadoop.conf.Configuration)
	 */
	@Override
	public void initialize(String argSpec, Configuration conf) throws IOException {
		// parse colon-delimited options
		String [] args = argSpec.split(":");

		// max ngram size
		mMaxGramSize = Integer.parseInt(args[0]);

		// temporal granularity
		mTemporalGranularity = args[1];
		
		// left and right temporal windows
		mLeftTimeWindow = Integer.parseInt(args[2]);
		mRightTimeWindow = Integer.parseInt(args[3]);

		// whether to use geo and/or temporal information as the context
		mGeo = Boolean.parseBoolean(args[4]);
		mTemporal = Boolean.parseBoolean(args[5]);
	}

	/* (non-Javadoc)
	 * @see edu.isi.mavuno.extract.Extractor#setDocument(edu.umd.cloud9.collection.Indexable)
	 */
	@Override
	public void setDocument(Indexable doc) {
		if(doc instanceof TwitterDocument) {
			TwitterDocument d = (TwitterDocument)doc;

			// get tweet time
			mCalendar.setTimeInMillis(d.getTimestamp());

			// get tweet location terms
			mLocation.set(d.getUserLocation().toLowerCase());

			// skip retweets
			if(!d.isRetweet()) {
				if(mGeo && !mTemporal) {
					loadGeoPairs(getTerms(doc));					
				}
				else if(!mGeo && mTemporal) {
					loadTemporalPairs(getTerms(doc));
				}
				else if(mGeo && mTemporal) {
					loadGeoTemporalPairs(getTerms(doc));
				}
				mGeoTemporalPairsIter = mGeoTemporalPairs.iterator();
			}
		}
		else {
			throw new RuntimeException("TwitterGeoTemporalExtractor is only applicable to TwitterDocument inputs!");
		}
	}

	/* (non-Javadoc)
	 * @see edu.isi.mavuno.extract.Extractor#getNextPair(edu.isi.mavuno.util.ContextPatternWritable)
	 */
	@Override
	public boolean getNextPair(ContextPatternWritable pair) {
		if(mGeoTemporalPairsIter != null && mGeoTemporalPairsIter.hasNext()) {
			ContextPatternWritable c = mGeoTemporalPairsIter.next();
			pair.setContext(c.getContext());
			pair.setPattern(c.getPattern());
			return true;
		}

		return false;
	}

	protected Text [] getTerms(Indexable doc) {
		String [] termsStr = doc.getContent().toLowerCase().split("\\s+"); // rudimentary tokenization
		Text [] termsTxt = new Text[termsStr.length];

		for(int i = 0; i < termsStr.length; i++) {
			termsTxt[i] = new Text(termsStr[i]);
		}

		return termsTxt;
	}

	protected boolean getPattern(Text pattern, Text [] terms, int start, int len) {
		if(start < 0 || start + len > terms.length) {
			return false;
		}

		pattern.clear();
		for(int i = start; i < start + len; i++) {
			pattern.append(terms[i].getBytes(), 0, terms[i].getLength());
			if(i != start + len - 1) {
				pattern.append(MavunoUtils.SPACE_BYTES, 0, MavunoUtils.SPACE_BYTES_LENGTH);
			}
		}

		return true;
	}

	private void loadGeoPairs(Text [] terms) {
		mGeoTemporalPairs.clear();

		ContextPatternWritable c;

		for(int i = 0; i < terms.length; i++) {
			for(int gramSize = 1; gramSize <= mMaxGramSize; gramSize++) {
				if(!getPattern(mPattern, terms, i, gramSize)) {
					continue;
				}

				c = new ContextPatternWritable();
				c.setPattern(mPattern);
				c.setContext(mLocation);
				mGeoTemporalPairs.add(c);
			}
		}
	}

	private void loadTemporalPairs(Text [] terms) {
		mGeoTemporalPairs.clear();

		ContextPatternWritable c;

		for(int i = 0; i < terms.length; i++) {
			for(int gramSize = 1; gramSize <= mMaxGramSize; gramSize++) {
				if(!getPattern(mPattern, terms, i, gramSize)) {
					continue;
				}

				// past contexts
				for(int leftTime = 1; leftTime <= mLeftTimeWindow; leftTime++) {
					c = new ContextPatternWritable();
					c.setContext(calToText());
					c.setPattern(mPattern);
					mGeoTemporalPairs.add(c);

					updateCalendar(-1);
				}

				// reset calendar
				updateCalendar(mLeftTimeWindow);

				// context for current time
				c = new ContextPatternWritable();
				c.setContext(calToText());
				c.setPattern(mPattern);
				mGeoTemporalPairs.add(c);

				// future contexts
				for(int rightTime = 1; rightTime <= mRightTimeWindow; rightTime++) {
					c = new ContextPatternWritable();
					c.setContext(calToText());
					c.setPattern(mPattern);
					mGeoTemporalPairs.add(c);

					updateCalendar(1);
				}

				// reset calendar
				updateCalendar(-mRightTimeWindow);
			}
		}
	}

	private void loadGeoTemporalPairs(Text [] terms) {
		mGeoTemporalPairs.clear();

		ContextPatternWritable c;

		for(int i = 0; i < terms.length; i++) {
			for(int gramSize = 1; gramSize <= mMaxGramSize; gramSize++) {
				if(!getPattern(mPattern, terms, i, gramSize)) {
					continue;
				}

				// past contexts
				for(int leftTime = 1; leftTime <= mLeftTimeWindow; leftTime++) {
					c = new ContextPatternWritable();
					c.setContext(MavunoUtils.createContext(calToText(), mLocation));
					c.setPattern(mPattern);
					mGeoTemporalPairs.add(c);

					updateCalendar(-1);
				}

				// reset calendar
				updateCalendar(mLeftTimeWindow);

				// context for current time
				c = new ContextPatternWritable();
				c.setContext(MavunoUtils.createContext(calToText(), mLocation));
				c.setPattern(mPattern);
				mGeoTemporalPairs.add(c);

				// future contexts
				for(int rightTime = 1; rightTime <= mRightTimeWindow; rightTime++) {
					c = new ContextPatternWritable();
					c.setContext(MavunoUtils.createContext(calToText(), mLocation));
					c.setPattern(mPattern);
					mGeoTemporalPairs.add(c);

					updateCalendar(1);
				}

				// reset calendar
				updateCalendar(-mRightTimeWindow);
			}
		}
	}

	private void updateCalendar(int amount) {
		if("year".equals(mTemporalGranularity)) {
			mCalendar.add(Calendar.YEAR, amount);
		}
		else if("month".equals(mTemporalGranularity)) {
			mCalendar.add(Calendar.MONTH, amount);
		}
		else if("day".equals(mTemporalGranularity)) {
			mCalendar.add(Calendar.DATE, amount);
		}
		else if("hour".equals(mTemporalGranularity)) {
			mCalendar.add(Calendar.HOUR_OF_DAY, amount);
		}
		else if("minute".equals(mTemporalGranularity)) {
			mCalendar.add(Calendar.MINUTE, amount);
		}
		else {
			mCalendar.add(Calendar.SECOND, amount);
		}
	}
	
	private Text calToText() {
		StringBuffer buf = new StringBuffer();
		buf.append(mCalendar.get(Calendar.YEAR));
		if("year".equals(mTemporalGranularity)) {
			return new Text(buf.toString());
		}
		buf.append(':');
		buf.append(mCalendar.get(Calendar.MONTH)+1);
		if("month".equals(mTemporalGranularity)) {
			return new Text(buf.toString());
		}
		buf.append(':');
		buf.append(mCalendar.get(Calendar.DATE));
		if("day".equals(mTemporalGranularity)) {
			return new Text(buf.toString());
		}
		buf.append(':');
		buf.append(mCalendar.get(Calendar.HOUR_OF_DAY));
		if("hour".equals(mTemporalGranularity)) {
			return new Text(buf.toString());
		}
		buf.append(':');
		buf.append(mCalendar.get(Calendar.MINUTE));
		if("minute".equals(mTemporalGranularity)) {
			return new Text(buf.toString());
		}
		buf.append(':');
		buf.append(mCalendar.get(Calendar.SECOND));
		return new Text(buf.toString());
	}
}

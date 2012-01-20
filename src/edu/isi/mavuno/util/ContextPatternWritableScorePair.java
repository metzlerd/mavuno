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

import org.apache.hadoop.io.DoubleWritable;

/**
 * @author metzler
 *
 */
public class ContextPatternWritableScorePair implements Comparable<ContextPatternWritableScorePair> {
	public final ContextPatternWritable contextpattern = new ContextPatternWritable();
	public final DoubleWritable score = new DoubleWritable();
	
	public ContextPatternWritableScorePair(ContextPatternWritable context, double score) {
		this.contextpattern.set(context);
		this.score.set(score);
	}

	@Override
	public int compareTo(ContextPatternWritableScorePair o) {
		double score = o.score.get();
		if(this.score.get() > score) {
			return 1;
		}
		else if(this.score.get() < score) {
			return -1;
		}
		else {
			return 0;
		}
	}
	
	@Override
	public boolean equals(Object o) {
		if(o == null || !(o instanceof ContextPatternWritableScorePair)) {
			return false;
		}
		return compareTo((ContextPatternWritableScorePair)o) == 0;
	}
	
	@Override
	public int hashCode() {
		return Double.valueOf(this.score.get()).hashCode();
	}
}

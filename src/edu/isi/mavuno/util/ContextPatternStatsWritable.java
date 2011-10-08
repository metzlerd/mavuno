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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * @author metzler
 *
 */
public class ContextPatternStatsWritable implements Writable {

	// (pattern, context) count
	public long matches;

	// (pattern) count
	public long globalPatternCount;
	
	// (context) count
	public long globalContextCount;

	// weight
	public double weight;
	
	public ContextPatternStatsWritable() {
		matches = 0L;
		globalPatternCount = 0L;
		globalContextCount = 0L;
		
		weight = 0.0;
	}

	public ContextPatternStatsWritable(long matches, long globalPatternCount, long globalContextCount, double domainScore, double weight) {
		this.matches = matches;
		this.globalPatternCount = globalPatternCount;
		this.globalContextCount = globalContextCount;
		this.weight = weight;
	}

	public void zero() {
		matches = 0L;
		globalPatternCount = 0L;
		globalContextCount = 0L;
		weight = 0.0;
	}
	
	public void increment(ContextPatternStatsWritable stats) {
		matches += stats.matches;
		globalPatternCount += stats.globalPatternCount;
		globalContextCount += stats.globalContextCount;
		weight = stats.weight;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		matches = in.readLong();
		globalPatternCount = in.readLong();
		globalContextCount = in.readLong();

		weight = in.readDouble();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(matches);
		out.writeLong(globalPatternCount);
		out.writeLong(globalContextCount);
		
		out.writeDouble(weight);
	}

	@Override
	public String toString() {
		StringBuilder str = new StringBuilder();
		str.append(matches);
		str.append('\t');
		str.append(globalPatternCount);
		str.append('\t');
		str.append(globalContextCount);
		str.append('\t');
		str.append(weight);
		return str.toString();
	}

	public void clear() {
		matches = 0L;
		globalPatternCount = 0L;
		globalContextCount = 0L;
		
		weight = 0.0;
	}
}

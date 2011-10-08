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
public class ScoreWritable implements Writable {

	// score
	public double score;
	
	// static score
	public double staticScore;
	
	// score normalization factor
	public double norm;
	
	public ScoreWritable() {
		score = 0.0;
		staticScore = 0.0;
		
		norm = 0.0;
	}
	
	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
		score = in.readDouble();
		staticScore = in.readDouble();

		norm = in.readDouble();
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeDouble(score);
		out.writeDouble(staticScore);

		out.writeDouble(norm);		
	}
	
	@Override
	public String toString() {
		StringBuilder str = new StringBuilder();
		str.append(score);
		str.append('\t');
		str.append(staticScore);
		str.append('\t');
		str.append(norm);
		return str.toString();
	}
}

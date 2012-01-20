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
import org.apache.hadoop.io.WritableComparable;

/**
 * @author metzler
 *
 */
public abstract class PairWritable<A extends Writable, B extends Writable> implements WritableComparable<PairWritable<A,B>> {

	public A left;
	public B right;
	
	public PairWritable() {
		super();
	}
	
	public PairWritable(A left, B right) {
		this.left = left;
		this.right = right;
	}
	
	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
		left.readFields(in);
		right.readFields(in);
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		left.write(out);
		right.write(out);
	}

	@Override
	public String toString() {
		return left.toString() + "\t" + right.toString();
	}

	@Override
	public abstract int compareTo(PairWritable<A, B> obj);
}

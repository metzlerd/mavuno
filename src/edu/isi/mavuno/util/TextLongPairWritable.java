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

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;

/**
 * @author metzler
 *
 */
public class TextLongPairWritable extends PairWritable<Text, LongWritable> {
	
	public TextLongPairWritable() {
		super(new Text(), new LongWritable());
	}

	@Override
	public int compareTo(PairWritable<Text, LongWritable> obj) {
		int cmp = this.left.compareTo(obj.left);
		if(cmp == 0) {
			return (int)(this.right.get() - obj.right.get());
		}
		return cmp;
	}

	public static final class Comparator extends WritableComparator {

		public Comparator() {
			super(TextLongPairWritable.class);
		}
		
		@Override
		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			try {
				int n1 = WritableUtils.decodeVIntSize(b1[s1]);
				int n2 = WritableUtils.decodeVIntSize(b2[s2]);
				int len1 = WritableComparator.readVInt(b1, s1);
				int len2 = WritableComparator.readVInt(b2, s2);
				int cmp = WritableComparator.compareBytes(b1, s1+n1, len1, b2, s2+n2, len2);
				if(cmp == 0) {
					long x1 = WritableComparator.readLong(b1, s1+n1+len1+1);
					long x2 = WritableComparator.readLong(b2, s2+n2+len2+1);
					return (int)(x1 - x2);
				}
				return cmp;
			}
			catch(IOException e) {
				throw new RuntimeException(e);
			}
		}

	}
	
	static {
		WritableComparator.define(TextLongPairWritable.class, new Comparator());
	}
}

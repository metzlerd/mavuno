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

import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Partitioner;


/**
 * @author metzler
 *
 */
public class ContextPatternWritable implements Writable {

	public static final String DELIM = "\t";
	public static final char DELIM_CHAR = '\t';

	public static final Text EOW = new Text("\u0001");
	public static final byte [] EOW_BYTES = EOW.getBytes();
	public static final int EOW_BYTES_LENGTH = EOW.getLength();

	public static final Text ASTERISK = new Text("\u0003");
	public static final String ASTERISK_STRING = "\u0003";

	public static final Text TAB = new Text("\t");
	public static final byte [] TAB_BYTES = TAB.getBytes();
	public static final int TAB_BYTES_LENGTH = TAB.getLength();

	public static final int TOTAL_FIELDS = 3;

	public static final int ID_FIELD = 0;
	public static final int CONTEXT_FIELD = 1;
	public static final int PATTERN_FIELD = 2;

	private static final int HEADER_LENGTH = TOTAL_FIELDS*(Integer.SIZE/8);

	private final Text mId = new Text();
	private final Text mContext = new Text();
	private final Text mPattern = new Text();

	private final Text mText = new Text();

	public ContextPatternWritable() {
		mId.set(ASTERISK);
		mContext.set(ASTERISK);
		mPattern.set(ASTERISK);
	}

	public ContextPatternWritable(String id, String context, String pattern) {
		mId.set(id);
		mPattern.set(pattern);
		mContext.set(context);
	}

	public ContextPatternWritable(ContextPatternWritable c) {
		mId.set(c.getId());
		mContext.set(c.getContext());
		mPattern.set(c.getPattern());
	}

	public final Text getId() {
		return mId;
	}

	public final Text getPattern() {
		return mPattern;
	}

	public final Text getContext() {
		return mContext;
	}

	public final int getLength() {
		return mId.getLength() + mContext.getLength() + mPattern.getLength() + TOTAL_FIELDS;
	}
	
	public void set(ContextPatternWritable context) {
		mId.set(context.getId());
		mContext.set(context.getContext());
		mPattern.set(context.getPattern());
	}

	public void setId(Text id) {
		mId.set(id);
	}

	public void setId(String id) {
		mId.set(id);
	}

	public void setPattern(Text pattern) {
		mPattern.set(pattern);
	}

	public void setPattern(String pattern) {
		mPattern.set(pattern);
	}

	public void setContext(Text context) {
		mContext.set(context);
	}

	public void setContext(String context) {
		mContext.set(context);
	}

	public void clear() {
		mId.set(ASTERISK);
		mContext.set(ASTERISK);
		mPattern.set(ASTERISK);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		int idLen = in.readInt();
		int contextLen = in.readInt();
		int patternLen = in.readInt();

		byte [] allBytes = new byte[TOTAL_FIELDS+idLen+contextLen+patternLen];
		in.readFully(allBytes);

		int offset = 0;

		mId.set(allBytes, offset, idLen);
		offset += idLen+1;

		mContext.set(allBytes, offset, contextLen);
		offset += contextLen+1;

		mPattern.set(allBytes, offset, patternLen);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(mId.getLength());
		out.writeInt(mContext.getLength());
		out.writeInt(mPattern.getLength());
		out.write(mId.getBytes(), 0, mId.getLength());
		out.write(EOW_BYTES, 0, EOW_BYTES_LENGTH);
		out.write(mContext.getBytes(), 0, mContext.getLength());
		out.write(EOW_BYTES, 0, EOW_BYTES_LENGTH);
		out.write(mPattern.getBytes(), 0, mPattern.getLength());
		out.write(EOW_BYTES, 0, EOW_BYTES_LENGTH);
	}

	public Text toText() {
		mText.clear();
		mText.append(mId.getBytes(), 0, mId.getLength());
		mText.append(TAB_BYTES, 0, TAB_BYTES_LENGTH);
		mText.append(mContext.getBytes(), 0, mContext.getLength());
		mText.append(TAB_BYTES, 0, TAB_BYTES_LENGTH);
		mText.append(mPattern.getBytes(), 0, mPattern.getLength());
		return mText;
	}

	@Override
	public String toString() {
		StringBuilder str = new StringBuilder();

		// construct string
		str.append(mId);
		str.append(DELIM_CHAR);
		str.append(mContext);
		str.append(DELIM_CHAR);
		str.append(mPattern);

		return str.toString();
	}

	@Override
	public int hashCode() {
		return getId().hashCode();
	}

	public static final class Comparator implements RawComparator<ContextPatternWritable> {

		@Override
		public int compare(ContextPatternWritable o1, ContextPatternWritable o2) {
			return o1.toText().compareTo(o2.toText());
		}

		@Override
		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			return WritableComparator.compareBytes(b1, s1+HEADER_LENGTH, l1-HEADER_LENGTH, b2, s2+HEADER_LENGTH, l2-HEADER_LENGTH);
		}

	}

	public static final class ContextComparator implements RawComparator<ContextPatternWritable> {

		@Override
		public int compare(ContextPatternWritable o1, ContextPatternWritable o2) {
			return o1.toText().compareTo(o2.toText());
		}

		@Override
		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			int idLen1 = WritableComparator.readInt(b1, s1) + 1;
			int idLen2 = WritableComparator.readInt(b2, s2) + 1;
			int cmp = WritableComparator.compareBytes(b1, s1+HEADER_LENGTH+idLen1, l1-HEADER_LENGTH-idLen1, b2, s2+HEADER_LENGTH+idLen2, l2-HEADER_LENGTH-idLen2);
			if(cmp == 0) {
				return WritableComparator.compareBytes(b1, s1+HEADER_LENGTH, idLen1, b2, s2+HEADER_LENGTH, idLen2);
			}
			return cmp;
		}

	}

	public static final class IdComparator implements RawComparator<ContextPatternWritable> {

		@Override
		public int compare(ContextPatternWritable o1, ContextPatternWritable o2) {
			return o1.toText().compareTo(o2.toText());
		}

		@Override
		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			int idlen1 = WritableComparator.readInt(b1, s1) + 1;
			int idlen2 = WritableComparator.readInt(b2, s2) + 1;
			int cmpA = WritableComparator.compareBytes(b1, s1+HEADER_LENGTH, idlen1, b2, s2+HEADER_LENGTH, idlen2);
			if(cmpA == 0) {
				return WritableComparator.compareBytes(b1, s1+HEADER_LENGTH+idlen1, l1-HEADER_LENGTH-idlen1, b2, s2+HEADER_LENGTH+idlen2, l2-HEADER_LENGTH-idlen2);
			}
			return cmpA;
		}

	}

	public static final class IdPatternComparator implements RawComparator<ContextPatternWritable> {

		@Override
		public int compare(ContextPatternWritable o1, ContextPatternWritable o2) {
			return o1.toText().compareTo(o2.toText());
		}

		@Override
		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			int idlen1 = WritableComparator.readInt(b1, s1) + 1;
			int idlen2 = WritableComparator.readInt(b2, s2) + 1;
			int cmpA = WritableComparator.compareBytes(b1, s1+HEADER_LENGTH, idlen1, b2, s2+HEADER_LENGTH, idlen2);
			if(cmpA == 0) {
				int contextlen1 = WritableComparator.readInt(b1, s1+(Integer.SIZE/8)) + 1;
				int patternlen1 = WritableComparator.readInt(b1, s1+2*(Integer.SIZE)/8) + 1;
				int contextlen2 = WritableComparator.readInt(b2, s2+(Integer.SIZE/8)) + 1;
				int patternlen2 = WritableComparator.readInt(b2, s2+2*(Integer.SIZE)/8) + 1;
				int cmpB = WritableComparator.compareBytes(b1, s1+HEADER_LENGTH+idlen1+contextlen1, patternlen1, b2, s2+HEADER_LENGTH+idlen2+contextlen2, patternlen2);
				if(cmpB == 0) {
					return WritableComparator.compareBytes(b1, s1+HEADER_LENGTH+idlen1, l1-HEADER_LENGTH-idlen1, b2, s2+HEADER_LENGTH+idlen2, l2-HEADER_LENGTH-idlen2);
				}
				return cmpB;
			}
			return cmpA;
		}

	}

	// most generic partitioner -- based on hashcode of entire object
	public static final class FullPartitioner extends Partitioner<ContextPatternWritable, Writable> {

		private final Text mText = new Text();

		@Override
		public int getPartition(ContextPatternWritable key, Writable value, int numPartitions) {
			// construct object
			mText.clear();
			mText.append(key.getId().getBytes(), 0, key.getId().getLength());
			mText.append(key.getContext().getBytes(), 0, key.getContext().getLength());
			mText.append(key.getPattern().getBytes(), 0, key.getPattern().getLength());

			// return hashcode mod number of partitions
			return MavunoUtils.mod(mText.hashCode(), numPartitions);
		}

	}

	// ensures that all objects with the same id are assigned the same partition
	public static final class IdPartitioner extends Partitioner<ContextPatternWritable, Writable> {

		private final Text mText = new Text();

		@Override
		public int getPartition(ContextPatternWritable key, Writable value, int numPartitions) {
			// construct object
			mText.clear();
			mText.append(key.getId().getBytes(), 0, key.getId().getLength());

			// return hashcode mod number of partitions
			return MavunoUtils.mod(mText.hashCode(), numPartitions);
		}
	}

	// ensures that all objects with the same (id, context) pair are assigned the same partition
	public static final class IdContextPartitioner extends Partitioner<ContextPatternWritable, Writable> {

		private final Text mText = new Text();

		@Override
		public int getPartition(ContextPatternWritable key, Writable value, int numPartitions) {
			// construct object
			mText.clear();
			mText.append(key.getId().getBytes(), 0, key.getId().getLength());
			mText.append(key.getContext().getBytes(), 0, key.getContext().getLength());

			// return hashcode mod number of partitions
			return MavunoUtils.mod(mText.hashCode(), numPartitions);
		}
	}

	// ensures that all objects with the same (id, pattern) pair are assigned the same partition
	public static final class IdPatternPartitioner extends Partitioner<ContextPatternWritable, Writable> {

		private final Text mText = new Text();

		@Override
		public int getPartition(ContextPatternWritable key, Writable value, int numPartitions) {
			// construct object
			mText.clear();
			mText.append(key.getId().getBytes(), 0, key.getId().getLength());
			mText.append(key.getPattern().getBytes(), 0, key.getPattern().getLength());

			// return hashcode mod number of partitions
			return MavunoUtils.mod(mText.hashCode(), numPartitions);
		}

	}	
}
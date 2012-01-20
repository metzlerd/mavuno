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

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Text;

/**
 * @author metzler
 *
 */
public class Relation {

	// relation name
	private final Text mName = new Text();
	
	// relation source
	private final Text mSource = new Text();
	
	// start position
	private int mStart;
	
	// end position
	private int mEnd;
	
	// arguments
	private Map<TypedTextSpan, Individual> mArguments = new HashMap<TypedTextSpan, Individual>();
	
	public Relation(Text name, Text source, int start, int end) {
		mName.set(name);
		mSource.set(source);
		mStart = start;
		mEnd = end;
	}
	
	public Text getName() {
		return mName;
	}

	public Text getSource() {
		return mSource;
	}

	public int getStartOffset() {
		return mStart;
	}
	
	public int getEndOffset() {
		return mEnd;
	}
	
	public Map<TypedTextSpan,Individual> getArguments() {
		return mArguments;
	}
	
	public void addArgument(Text type, Text text, Individual individual, int begin, int end) {
		mArguments.put(new TypedTextSpan(type, text, begin, end), individual);
	}
	
//	@Override
//	public int compareTo(Relation o) {
//		return toString().compareTo(o.toString());
//	}

	@Override
	public int hashCode() {
		return toString().hashCode();
	}
	
	@Override
	public boolean equals(Object o) {
		if(o == null) {
			return false;
		}
		return toString().compareTo(o.toString()) == 0;
	}
	
	@Override
	public String toString() {
		StringBuffer buf = new StringBuffer();
		
		buf.append("[name: ");
		buf.append(mName);
		buf.append(", source:");
		buf.append(mSource);
		buf.append(", start: ");
		buf.append(mStart);
		buf.append(", end: ");
		buf.append(mEnd);
		
		for(TypedTextSpan span : mArguments.keySet()) {
			buf.append(", arg: [span: ");
			buf.append(span);
			buf.append(", individual: ");
			buf.append(mArguments.get(span));
			buf.append(']');
		}
		
		buf.append(']');
		
		return buf.toString();
	}
}
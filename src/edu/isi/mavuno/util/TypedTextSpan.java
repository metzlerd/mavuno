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

import org.apache.hadoop.io.Text;

/**
 * @author metzler
 *
 */
public class TypedTextSpan {
	// span type
	public final Text type = new Text();

	// span text
	public final Text text = new Text();

	// span start and end character offsets
	public int start;
	public int end;

	public TypedTextSpan(Text type, Text text, int start, int end) {
		if(type != null) {
			this.type.set(type);
		}
		if(text != null) {
			this.text.set(text);
		}
		this.start = start;
		this.end = end;
	}
	
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

		buf.append("[type: ");
		buf.append(type);
		buf.append(", text: ");
		buf.append(text);
		buf.append(", start: ");
		buf.append(start);
		buf.append(", end: ");
		buf.append(end);
		buf.append(']');
		
		return buf.toString();
	}
}

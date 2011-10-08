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

import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.Text;

/**
 * @author metzler
 *
 */
public class Individual {

	// canonical form of the individual
	private final TypedTextSpan mSpan;

	// occurrences of this individual
	private final Set<TypedTextSpan> mOccurrences = new HashSet<TypedTextSpan>();

	public Individual(Text name, int start, int end) {
		mSpan = new TypedTextSpan(null, name, start, end);
		mOccurrences.clear();
	}

	public TypedTextSpan getSpan() {
		return mSpan;
	}

	public Set<TypedTextSpan> getOccurrences() {
		return mOccurrences;
	}
	
	public void addOccurrence(TypedTextSpan occurrence) {
		mOccurrences.add(occurrence);
	}	
	
	@Override
	public String toString() {
		StringBuffer buf = new StringBuffer();
		
		buf.append("[span: ");
		buf.append(mSpan);
		for(TypedTextSpan span : mOccurrences) {
			buf.append(", occurrence: ");
			buf.append(span);
		}
		buf.append(']');
		
		return buf.toString();
	}
}

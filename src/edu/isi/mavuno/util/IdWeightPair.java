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
public class IdWeightPair implements Comparable<IdWeightPair> {
	public final Text id = new Text();
	public float weight = 0.0f;
	
	public IdWeightPair(Text id, float weight) {
		this.id.set(id);
		this.weight = weight;
	}

	@Override
	public int compareTo(IdWeightPair o) {
		float w = o.weight;
		if(this.weight > w) {
			return -1;
		}
		else if(this.weight < w) {
			return 1;
		}
		else {
			return 0;
		}
	}
	
	@Override
	public boolean equals(Object o) {
		if(o == null || !(o instanceof IdWeightPair)) {
			return false;
		}
		return compareTo((IdWeightPair)o) == 0;
	}
	
	@Override
	public int hashCode() {
		return Float.valueOf(weight).hashCode();
	}
	
	@Override
	public String toString() {
		return "[id=" + id + ", weight=" + weight + "]";
	}
}

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

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 * @author metzler
 *
 */
public class TokenWritable implements Writable {

	public static final String SEP = "\t";

	// token
	private final Text mToken = new Text();
	
	public TokenWritable() {
		mToken.clear();
	}
		
	public void setToken(Text token) {
		safeSet(mToken, token);
	}

	public void setToken(String token) {
		safeSet(mToken, token);
	}

	public Text getToken() {
		return mToken;
	}

	protected static void safeSet(Text t, Text s) {
		if(s == null) {
			t.clear();
		}
		else {
			t.set(s);
		}		
	}
	
	protected static void safeSet(Text t, String s) {
		if(s == null) {
			t.clear();
		}
		else {
			t.set(s);
		}
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
		mToken.readFields(in);
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		mToken.write(out);
	}

	@Override
	public String toString() {
		return mToken.toString();
	}	
}

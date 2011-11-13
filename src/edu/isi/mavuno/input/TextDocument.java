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

package edu.isi.mavuno.input;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


/**
 * @author metzler
 *
 */
public class TextDocument extends Indexable {
	
	private String mDocId;
	private String mText;
	
	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
		mDocId = in.readUTF();
		mText = in.readUTF();
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(mDocId);
		out.writeUTF(mText);
	}

	/* (non-Javadoc)
	 * @see edu.umd.cloud9.collection.Indexable#getContent()
	 */
	@Override
	public String getContent() {
		return mText;
	}

	/* (non-Javadoc)
	 * @see edu.umd.cloud9.collection.Indexable#getDocid()
	 */
	@Override
	public String getDocid() {
		return mDocId;
	}

	public void set(String docid, String text) {
		mDocId = docid;
		mText = text;
	}

}

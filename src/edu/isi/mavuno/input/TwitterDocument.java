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
import java.text.ParseException;
import java.text.SimpleDateFormat;

import org.apache.hadoop.io.Text;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;


/**
 * @author metzler
 *
 */
public class TwitterDocument extends Indexable {

	// date parser
	private static final SimpleDateFormat DATE_PARSER = new SimpleDateFormat("EEE MMM d HH:mm:ss Z yyyy");

	// tweet id
	private long mTweetId;

	// user id
	private String mUserScreenName;

	// user location (text)
	private String mUserLocation;

	// user location (geo-based)
	private double mUserLongitude;
	private double mUserLatitude;

	// text of tweet
	private String mText;

	// timestamp
	private long mTimestamp;

	// is this a retweet?
	private boolean mIsRetweet;

	// raw JSON
	private final Text mRawDoc = new Text();

	@Override
	public void readFields(DataInput in) throws IOException {
		mRawDoc.readFields(in);
		try {
			initialize();
		}
		catch(JSONException e) {
			throw new RuntimeException("Error deserializing TwitterDocument --" + e);
		}
	}

	@Override
	public String getContent() {
		return mText;
	}

	@Override
	public String getDocid() {
		return Long.toString(mTweetId);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		mRawDoc.write(out);
	}

	public void set(Text doc) throws JSONException {
		mRawDoc.set(doc);
		initialize();
	}

	public String getUserScreenName() {
		return mUserScreenName;
	}

	public long getTimestamp() {
		return mTimestamp;
	}

	public String getUserLocation() {
		return mUserLocation;
	}

	public boolean isRetweet() {
		return mIsRetweet;
	}

	private void initialize() throws JSONException {
		// get json representation of the tweet
		JSONObject jobj = new JSONObject(mRawDoc.toString());

		// get tweet id 
		if(jobj.has("id")) {
			mTweetId = jobj.getLong("id");
		}
		else {
			mTweetId = 0;
		}

		// get tweet text
		if(jobj.has("text")) {
			mText = jobj.getString("text");
		}
		else {
			mText = "";
		}

		// get tweet timestamp
		if(jobj.has("created_at")) {
			try {
				mTimestamp = DATE_PARSER.parse(jobj.getString("created_at")).getTime();
			}
			catch (ParseException e) {
				mTimestamp = 0L;
			}
		}
		else {
			mTimestamp = 0L;
		}

		// is this a retweet?
		if(jobj.has("retweeted")) {
			mIsRetweet = jobj.getBoolean("retweeted") || (mText != null && mText.toLowerCase().startsWith("rt "));
		}
		else {
			mIsRetweet = false;
		}

		// get user information
		if(jobj.has("user")) {
			JSONObject userinfo = jobj.getJSONObject("user");
			if(userinfo.has("screen_name")) {
				mUserScreenName = userinfo.getString("screen_name");
			}
			if(userinfo.has("location")) {
				mUserLocation = userinfo.getString("location");
			}
			else {
				mUserLocation = "";
			}
		}

		// get geo information
		if(jobj.has("geo")) {
			try {
				JSONObject geoinfo = jobj.getJSONObject("geo");
				if(geoinfo.has("type") && geoinfo.getString("type").equals("Point") && geoinfo.has("coordinates")) {
					JSONArray coords = geoinfo.getJSONArray("coordinates");
					mUserLongitude = coords.getDouble(0);
					mUserLatitude = coords.getDouble(1);
				}
			}
			catch(JSONException e) {
				mUserLongitude = 0.0;
				mUserLatitude = 0.0;
			}
		}
	}

	@Override
	public String toString() {
		StringBuffer buf = new StringBuffer();
		buf.append("<id>").append(mTweetId).append("</id>");
		buf.append("<text>").append(mText).append("</text>");
		buf.append("<timestamp>").append(mTimestamp).append("</timestamp>");
		buf.append("<isretweet>").append(mIsRetweet).append("</isretweet>");
		buf.append("<userscreenname>").append(mUserScreenName).append("</userscreenname>");
		buf.append("<userlocation>").append(mUserLocation).append("</userlocation>");
		buf.append("<userlongitude>").append(mUserLongitude).append("</userlongitude>");
		buf.append("<userlatitude>").append(mUserLatitude).append("</userlatitude>");
		return buf.toString();
	}

}

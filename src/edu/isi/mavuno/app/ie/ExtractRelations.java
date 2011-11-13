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

package edu.isi.mavuno.app.ie;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import edu.isi.mavuno.extract.Extractor;
import edu.isi.mavuno.input.SentenceSegmentedDocument;
import edu.isi.mavuno.util.ContextPatternWritable;
import edu.isi.mavuno.util.IdWeightPair;
import edu.isi.mavuno.util.Individual;
import edu.isi.mavuno.util.MavunoUtils;
import edu.isi.mavuno.util.Relation;
import edu.isi.mavuno.util.SentenceWritable;
import edu.isi.mavuno.util.TokenFactory;
import edu.isi.mavuno.util.TratzParsedTokenWritable;
import edu.isi.mavuno.util.TypedTextSpan;
import edu.stanford.nlp.util.IntPair;
import edu.stanford.nlp.util.Pair;

/**
 * @author metzler
 *
 */
public class ExtractRelations extends Configured implements Tool {
	private static final Logger sLogger = Logger.getLogger(ExtractRelations.class);

	private static final TokenFactory<TratzParsedTokenWritable> TOKEN_FACTORY = new TratzParsedTokenWritable.ParsedTokenFactory();

	public ExtractRelations(Configuration conf) {
		super(conf);
	}

	private static class MyMapper extends Mapper<Writable, SentenceSegmentedDocument<TratzParsedTokenWritable>, Text, Text> {

		private static final Text MAVUNO_SOURCE = new Text("isi:mavuno");

		private static final Text DEFAULT_TYPE = new Text("ANY");
		private static final Text O = new Text("O");

		private static byte [] BUFFER = new byte[1024*1024];

		private String mPlaintextPath = null;

		private Extractor mExtractor = null;

		private final Map<Text, Text> mRelationNameLookup = new HashMap<Text, Text>();

		private final Map<Text, Text []> mArgNames = new HashMap<Text, Text []>();
		private final Map<Text, Text []> mArgTypes = new HashMap<Text, Text []>();
		private final Map<Text, Text []> mArgClasses = new HashMap<Text, Text []>();

		private final Map<Text, Text> mPrimaryClasses = new HashMap<Text, Text>();
		private final Map<Text, List<IdWeightPair>> mPatterns =  new HashMap<Text, List<IdWeightPair>>();
		private final Map<Text, List<IdWeightPair>> mInstances = new HashMap<Text, List<IdWeightPair>>();

		// maps relations to their confidence scores
		private final Map<Relation,Double> mRelations = new HashMap<Relation,Double>();

		// maps individual ids to their representations
		private final Map<Text,Individual> mIndividuals = new HashMap<Text,Individual>();

		private final SentenceSegmentedDocument<TratzParsedTokenWritable> mDoc = new SentenceSegmentedDocument<TratzParsedTokenWritable>(TOKEN_FACTORY);

		private final Text mKey = new Text();
		private final Text mValue = new Text();

		private final ContextPatternWritable mPair = new ContextPatternWritable();

		//		private final Map<IntPair, Integer> mCorefForwardLookup = new HashMap<IntPair, Integer>();
		//		private final Map<Integer, List<IntPair>> mCorefReverseLookup = new HashMap<Integer, List<IntPair>>();

		private void loadTypes(String typesPath, Configuration conf) throws IOException {
			// reset relation name lookup
			mRelationNameLookup.clear();

			// clear argument names
			mArgNames.clear();

			// clear argument types
			mArgTypes.clear();

			// clear argument classes
			mArgClasses.clear();

			BufferedReader reader = MavunoUtils.getBufferedReader(conf, typesPath);

			// read types
			String input;
			while((input = reader.readLine()) != null) {
				String [] cols = input.split("\t");

				if(cols.length < 5 || (cols.length - 2) % 3 != 0) {
					throw new RuntimeException("Ill-formed line in types file -- " + input);
				}

				Text relationId = new Text(cols[0]);
				Text relationName = new Text(cols[1]);

				mRelationNameLookup.put(relationId, relationName);

				Text [] argNames = new Text[(cols.length-2)/3];
				Text [] argTypes = new Text[(cols.length-2)/3];
				Text [] argClasses = new Text[(cols.length-2)/3];

				for(int i = 2; i < cols.length; i+=3) {
					argNames[(i-2)/3] = new Text(cols[i]);
					argTypes[(i-2)/3] = new Text(cols[i+1]);
					argClasses[(i-2)/3] = new Text(cols[i+2]);
				}

				mArgNames.put(relationId, argNames);
				mArgTypes.put(relationId, argTypes);
				mArgClasses.put(relationId, argClasses);
			}

			// close current reader
			reader.close();
		}

		private static void loadPatterns(Map<Text, List<IdWeightPair>> patterns, String patternsPath, Configuration conf) throws IOException {
			// clear example lookup
			patterns.clear();

			BufferedReader reader = MavunoUtils.getBufferedReader(conf, patternsPath);

			// read patterns
			String input;
			while((input = reader.readLine()) != null) {
				String [] cols = input.split("\t");

				if(cols.length < 2 || cols.length > 3) {
					throw new RuntimeException("Ill-formed line in pattern file -- " + input);
				}

				Text relationName = new Text(cols[0]);
				Text pattern = new Text(cols[1]);

				float weight = 1.0f;
				if(cols.length == 3) {
					weight = Float.parseFloat(cols[2]);
				}

				IdWeightPair pair = new IdWeightPair(relationName, weight);

				updatePatternMap(patterns, pattern, pair);
			}

			// close current reader
			reader.close();
		}

		private static void updatePatternMap(Map<Text, List<IdWeightPair>> patterns, Text pattern, IdWeightPair pair) {
			// populate pattern lookup
			List<IdWeightPair> contextList = null;
			contextList = patterns.get(pattern);
			if(contextList == null) {
				contextList = new ArrayList<IdWeightPair>(1);
				contextList.add(pair);
				patterns.put(pattern, contextList);
			}
			else {
				contextList.add(pair);
			}
		}

		@Override
		public void setup(Mapper<Writable, SentenceSegmentedDocument<TratzParsedTokenWritable>, Text, Text>.Context context) throws IOException {
			Configuration conf = context.getConfiguration();

			try {
				// initialize extractor
				mExtractor = (Extractor)Class.forName(conf.get("Mavuno.ExtractRelations.ExtractorClass")).newInstance();
				String contextArgs = conf.get("Mavuno.ExtractRelations.ExtractorArgs", null);
				mExtractor.initialize(contextArgs, conf);

				// load types
				String typesPath = conf.get("Mavuno.ExtractRelations.TypesPath", null);
				loadTypes(typesPath, conf);

				// get primary types
				String [] primaryTypes = conf.get("Mavuno.ExtractRelations.PrimaryTypes", "").split(",");
				mPrimaryClasses.clear();
				for(int i = 0; i < primaryTypes.length; i++) {
					String [] pair = primaryTypes[i].split("\\|");
					if(pair.length != 2) {
						throw new RuntimeException("Illegal primary type specification -- " + primaryTypes[i]);
					}
					mPrimaryClasses.put(new Text(pair[0]), new Text(pair[1]));
				}

				// load extraction patterns
				String patternsPath = conf.get("Mavuno.ExtractRelations.PatternsPath", null);
				loadPatterns(mPatterns, patternsPath, conf);

				// load instances (if provided)
				String instancesPath = conf.get("Mavuno.ExtractRelations.InstancesPath", null);
				if(instancesPath != null) {
					loadPatterns(mInstances, instancesPath, conf);
				}

				// set plaintext corpus path location (if set)
				mPlaintextPath = conf.get("Mavuno.ExtractRelations.PlaintextPath", null);
			}
			catch(Exception e) {
				throw new RuntimeException(e);
			}
		}

		@Override
		public void map(Writable key, SentenceSegmentedDocument<TratzParsedTokenWritable> doc, Mapper<Writable, SentenceSegmentedDocument<TratzParsedTokenWritable>, Text, Text>.Context context) throws IOException, InterruptedException {
			// key = document id
			mKey.set(doc.getDocid());

			sLogger.info("Processing document: " + doc.getDocid());

			// get sentences
			List<SentenceWritable<TratzParsedTokenWritable>> sentences = doc.getSentences();

			int sentId = 0;
			//int tokenId = 0;

			//			// get coref clusters
			//			mCorefForwardLookup.clear();
			//			mCorefReverseLookup.clear();
			//
			//			for(SentenceWritable<TratzParsedTokenWritable> s : sentences) {
			//				// reset token id
			//				tokenId = 0;
			//
			//				for(TratzParsedTokenWritable t : s.getTokens()) {
			//					int id = t.getCorefId();
			//					if(id != -1) {
			//						// position within the document
			//						IntPair pos = new IntPair(sentId, tokenId);
			//
			//						// forward lookup
			//						mCorefForwardLookup.put(pos, id);
			//
			//						// reverse lookup
			//						List<IntPair> pairs = mCorefReverseLookup.get(id);
			//						if(pairs == null) {
			//							pairs = new ArrayList<IntPair>();
			//							pairs.add(pos);
			//							mCorefReverseLookup.put(id, pairs);
			//						}
			//						else {
			//							pairs.add(pos);
			//						}
			//					}
			//					tokenId++;
			//				}
			//				sentId++;
			//			}

			// clear relations
			mRelations.clear();

			// clear individuals
			mIndividuals.clear();

			// extract separately from each sentence
			sentId = 0;
			//tokenId = 0;
			for(SentenceWritable<TratzParsedTokenWritable> s : sentences) {
				// construct new document that only contains this sentence
				mDoc.clear();
				mDoc.addSentence(s);

				// set current document
				mExtractor.setDocument(mDoc);

				//sLogger.info("SENTENCE = " + s);

				String sentenceText = s.toStringOfTokens();

				// skip empty sentences
				if(sentenceText.length() == 0) {
					continue;
				}

				int sentenceCharOffsetBegin = s.getTokenAt(0).getCharOffsetBegin();
				int sentenceCharOffsetEnd = s.getTokenAt(s.getNumTokens()-1).getCharOffsetEnd();

				List<Text> arguments = new ArrayList<Text>();

				// reset token id
				//tokenId = 0;

				// main extract loop -- extracts instances and relations
				List<IdWeightPair> contextList = null;
				while(mExtractor.getNextPair(mPair)) {
					// get the context instances for this extraction
					arguments.clear();
					String [] args = mPair.getContext().toString().split("\\|");
					for(int i = 0; i < args.length; i++) {
						arguments.add(new Text(args[i]));							
					}

					List<Set<Text>> allArgClasses = new ArrayList<Set<Text>>();
					List<Integer> allArgCharOffsetBegin = new ArrayList<Integer>();
					List<Integer> allArgCharOffsetEnd = new ArrayList<Integer>();

					// process each argument instance
					for(Text arg : arguments) {
						// get offset within sentence
						int argOffset = getOffset(arg, sentenceText);

						// skip if we can't find an alignment for some reason
						if(argOffset == -1) {
							sLogger.warn("Can't find alignment for: " + arg + " in sentence: " + sentenceText);
							continue;
						}

						// argument length
						int argLength = getLength(arg);

						// argument char offsets
						int argCharOffsetBegin = s.getTokenAt(argOffset).getCharOffsetBegin();
						int argCharOffsetEnd = s.getTokenAt(argOffset+argLength-1).getCharOffsetEnd();

						allArgCharOffsetBegin.add(argCharOffsetBegin);
						allArgCharOffsetEnd.add(argCharOffsetEnd);

						// get arg classes
						Set<Text> argClasses = getTypes(sentences, s, sentId, argOffset, argLength, true);

						allArgClasses.add(argClasses);

						// is this a mention of a primary class? if so, then process it
						for(Text argClass : argClasses) {
							Text argType = mPrimaryClasses.get(argClass);
							if(argType != null) {
								Pair<Text,IntPair> individualSpec = resolveCoref(mPair.getContext(), argClass, sentences, s, argOffset, argLength);
								Individual individual = mIndividuals.get(individualSpec.first);
								if(individual == null) {
									individual = new Individual(individualSpec.first, individualSpec.second.getSource(), individualSpec.second.getTarget());
									mIndividuals.put(new Text(individualSpec.first), individual);
								}
								individual.addOccurrence(new TypedTextSpan(argType, individualSpec.first, individualSpec.second.getSource(), individualSpec.second.getTarget()));
							}
						}
					}

					// check if this pattern matches any of the relation patterns
					contextList = mPatterns.get(mPair.getPattern());

					// if this pattern doesn't match any of the relation patterns then we're done
					if(contextList == null) {
						continue;
					}

					// if found, then process
					for(IdWeightPair pair : contextList) {
						Text [] expectedNames = mArgNames.get(pair.id);
						Text [] expectedTypes = mArgTypes.get(pair.id);
						Text [] expectedClasses = mArgClasses.get(pair.id);
						
						// uh oh, we're missing name and/or type information
						if(expectedNames == null || expectedTypes == null || expectedClasses == null) {
							throw new RuntimeException("Missing name, type, and/or class information for: " + pair);
						}

						// perform length count checking
						if(expectedClasses.length != expectedNames.length || expectedNames.length != expectedTypes.length || expectedTypes.length != allArgClasses.size()) {
							sLogger.warn("Argument length mismatch for: " + pair + " -- skipping!");
							continue;
						}

						// perform class type checking
						boolean matches = true;
						for(int i = 0; i < expectedClasses.length; i++) {
							if(!allArgClasses.get(i).contains(expectedClasses[i])) {
								matches = false;
								break;
							}
						}

						if(matches) {
							// build relation
							Relation r = new Relation(mRelationNameLookup.get(pair.id), MAVUNO_SOURCE, sentenceCharOffsetBegin, sentenceCharOffsetEnd);
							for(int i = 0; i < arguments.size(); i++) {
								// argument text
								Text argText = arguments.get(i);

								// argument name
								Text argName = expectedNames[i];

								// argument type
								Text argType = expectedTypes[i];

								// beginning and end offset for this argument
								int argBegin = allArgCharOffsetBegin.get(i);
								int argEnd = allArgCharOffsetEnd.get(i);

								// find the individual for this argument
								Individual individual = mIndividuals.get(argText);
								if(individual == null) { // create new individual
									individual = new Individual(argText, argBegin, argEnd);
									individual.addOccurrence(new TypedTextSpan(argType, argText, argBegin, argEnd));
									mIndividuals.put(new Text(argText), individual);
								}
								else {
									individual.addOccurrence(new TypedTextSpan(argType, argText, argBegin, argEnd));
								}

								// add argument to relation
								r.addArgument(argName, argText, individual, argBegin, argEnd);
							}

							Double confidence = mRelations.get(r);
							if(confidence == null) {
								mRelations.put(r, new Double(pair.weight));
							}
							else {
								mRelations.put(r, confidence + pair.weight);
							}
						}
					}

					//tokenId++;
				}

				sentId++;
			}

			try {
				// read plain text version of document, if necessary
				String documentText = null;
				if(mPlaintextPath != null) {
					documentText = loadDocumentText(context.getConfiguration(), mPlaintextPath, doc.getDocid());
				}

				// generate XML output
				String xml = getXMLOutput(doc.getDocid(), documentText, mRelations, mIndividuals);
				//System.out.println(xml);
				mValue.set(xml);
				context.write(mKey, mValue);
			}
			catch(Exception e) {
				throw new RuntimeException(e);
			}
		}

		private static String loadDocumentText(Configuration conf, String path, String docid) throws IOException {
			Text text = new Text();

			FSDataInputStream reader = MavunoUtils.getFSDataInputStream(conf, path + "/" + docid);

			int n;
			while((n = reader.read(BUFFER, 0, BUFFER.length)) != -1) {
				text.append(BUFFER, 0, n);
			}

			reader.close();

			return text.toString();
		}

		// creates an XML representation of the relations and individuals
		private static String getXMLOutput(String docid, String docText, Map<Relation,Double> relations, Map<Text,Individual> individuals) throws ParserConfigurationException, TransformerException {			
			DocumentBuilderFactory domFactory = DocumentBuilderFactory.newInstance();
			DocumentBuilder domBuilder = domFactory.newDocumentBuilder();

			Document doc = domBuilder.newDocument();
			Element rootElement = doc.createElement("doc");
			rootElement.setAttribute("xmlns", "http://www.bbn.com/MR/ELF");
			rootElement.setAttribute("id", docid);
			rootElement.setAttribute("elf-version", "2.2");
			rootElement.setAttribute("source", "Mavuno Reader");
			rootElement.setAttribute("contents", "S-ELF");
			doc.appendChild(rootElement);

			Map<Individual,Integer> individualIds = new HashMap<Individual,Integer>();

			int id = 1;
			for(Map.Entry<Text,Individual> indEntry: individuals.entrySet()) {
				Individual ind = indEntry.getValue();
				TypedTextSpan indSpan = ind.getSpan();

				Element indElement = doc.createElement("individual");
				indElement.setAttribute("id", Integer.toString(id));

				Element nameElement = doc.createElement("name");
				if(docText != null) {
					nameElement.setTextContent(docText.substring(indSpan.start, indSpan.end+1));
				}
				nameElement.setAttribute("name", indSpan.text.toString());
				nameElement.setAttribute("start", Integer.toString(indSpan.start));
				nameElement.setAttribute("end", Integer.toString(indSpan.end));
				indElement.appendChild(nameElement);

				for(TypedTextSpan occurrence : ind.getOccurrences()) {
					// handle special case
					// TODO: make this more modular
					if(occurrence.type.toString().equals("xsd:string")) {
						continue;
					}

					Element occurrenceElement = doc.createElement("type");
					if(docText != null) {
						occurrenceElement.setTextContent(docText.substring(occurrence.start, occurrence.end+1));
					}
					occurrenceElement.setAttribute("type", occurrence.type.toString());
					occurrenceElement.setAttribute("start", Integer.toString(occurrence.start));
					occurrenceElement.setAttribute("end", Integer.toString(occurrence.end));
					indElement.appendChild(occurrenceElement);					
				}

				if(indElement.getChildNodes().getLength() > 1) {
					rootElement.appendChild(indElement);
					individualIds.put(ind, id);
					id++;
				}				
			}

			for(Map.Entry<Relation,Double> relEntry: relations.entrySet()) {
				Relation rel = relEntry.getKey();
				double confidence = relEntry.getValue();

				// TODO: fix this
				if(confidence > 1.0) { confidence = 1.0; }

				Element relationElement = doc.createElement("relation");
				if(docText != null) {
					Element textElement = doc.createElement("text");
					textElement.setTextContent(docText.substring(rel.getStartOffset(), rel.getEndOffset()+1));
					relationElement.appendChild(textElement);
				}
				relationElement.setAttribute("name", rel.getName().toString());
				relationElement.setAttribute("source", rel.getSource().toString());
				relationElement.setAttribute("start", Integer.toString(rel.getStartOffset()));
				relationElement.setAttribute("end", Integer.toString(rel.getEndOffset()));
				relationElement.setAttribute("p", Double.toString(confidence));

				for(Map.Entry<TypedTextSpan, Individual> argEntry : rel.getArguments().entrySet()) {
					TypedTextSpan argSpan = argEntry.getKey();
					Individual argInd = argEntry.getValue();

					Element argumentElement = doc.createElement("arg");
					if(docText != null) {
						argumentElement.setTextContent(docText.substring(argSpan.start, argSpan.end+1));
					}
					argumentElement.setAttribute("role", argSpan.type.toString());
					argumentElement.setAttribute("start", Integer.toString(argSpan.start));
					argumentElement.setAttribute("end", Integer.toString(argSpan.end));

					// handle special case
					// TODO: make this more modular
					if(argSpan.type.toString().startsWith("t:")) {
						argumentElement.setAttribute("type", "xsd:string");
						argumentElement.setAttribute("value", argSpan.text.toString());						
					}
					else {
						int argId = individualIds.get(argInd);
						argumentElement.setAttribute("id", Integer.toString(argId));
					}

					relationElement.appendChild(argumentElement);
				}

				rootElement.appendChild(relationElement);
			}

			TransformerFactory transFactory = TransformerFactory.newInstance();
			Transformer trans = transFactory.newTransformer();
			trans.setOutputProperty(OutputKeys.INDENT, "yes");
			trans.setOutputProperty("{http://xml.apache.org/xslt}indent-amount", "2");

			StringWriter sw = new StringWriter();
			StreamResult result = new StreamResult(sw);
			DOMSource source = new DOMSource(doc);
			trans.transform(source, result);

			return sw.toString();
		}

		private Pair<Text, IntPair> resolveCoref(Text arg, Text expectedType, List<SentenceWritable<TratzParsedTokenWritable>> sentences, SentenceWritable<TratzParsedTokenWritable> s, int offset, int length) {
			Pair<Text,IntPair> bestPair = null;
			//			// resolve co-ref to best individual
			//			for(int i = offset; i < offset + length; i++) {
			//				int corefId = s.getTokenAt(i).getCorefId();
			//				List<IntPair> pairs = mCorefReverseLookup.get(corefId);
			//				if(pairs != null) {
			//					for(IntPair p : pairs) {
			//						IntPair chunkSpan = getChunkSpan(sentences.get(p.getSource()), p.getTarget());
			//						Set<Text> chunkTypes = getTypes(sentences, sentences.get(p.getSource()), p.getSource(), chunkSpan.getSource(), chunkSpan.getTarget(), false);
			//						if(chunkTypes.contains(expectedType)) {
			//							Text chunkText = getSpan(sentences.get(p.getSource()), chunkSpan.getSource(), chunkSpan.getTarget());
			//							int begin = sentences.get(p.getSource()).getTokenAt(chunkSpan.getSource()).getCharOffsetBegin();
			//							int end = sentences.get(p.getSource()).getTokenAt(chunkSpan.getSource()+chunkSpan.getTarget()-1).getCharOffsetEnd();
			//							//System.out.println(arg + " RESOLVES TO " + chunkText + "\t" + begin + "\t" + end);
			//							if(bestPair == null || chunkText.getLength() > bestPair.first.getLength()) {
			//								bestPair = new Pair<Text,IntPair>(chunkText, new IntPair(begin, end));
			//							}
			//						}
			//					}
			//				}
			//			}
			//
			if(bestPair == null) {
				IntPair chunkSpan = getChunkSpan(s, offset);
				Text chunkText = getSpan(s, chunkSpan.getSource(), chunkSpan.getTarget());
				int begin = s.getTokenAt(chunkSpan.getSource()).getCharOffsetBegin();
				int end = s.getTokenAt(chunkSpan.getSource()+chunkSpan.getTarget()-1).getCharOffsetEnd();
				bestPair = new Pair<Text,IntPair>(chunkText, new IntPair(begin, end));
			}

			return bestPair;
		}

		private Text getSpan(SentenceWritable<TratzParsedTokenWritable> tokens, int offset, int length) {
			Text span = new Text();

			for(int i = offset; i < offset + length; i++) {
				Text t = tokens.getTokenAt(i).getToken();
				span.append(t.getBytes(), 0, t.getLength());
				if(i != offset + length - 1) {
					span.append(MavunoUtils.SPACE_BYTES, 0, MavunoUtils.SPACE_BYTES_LENGTH);
				}
			}

			return span;
		}

		private IntPair getChunkSpan(SentenceWritable<TratzParsedTokenWritable> tokens, int offset) {
			int beginPos = 0;

			Text curNETag = null;
			for(int i = 0; i < tokens.getNumTokens(); i++) {
				Text neTag = tokens.getTokenAt(i).getNETag();
				Text chunkTag = tokens.getTokenAt(i).getChunkTag();

				// don't split named entities
				if(curNETag == null || (!curNETag.equals(O) && !neTag.equals(curNETag)) || (curNETag.equals(O) && (chunkTag.getBytes()[0] == 'B' || chunkTag.getBytes()[0] == 'O'))) {
					if(i > offset) {
						return new IntPair(beginPos, i - beginPos);
					}
					curNETag = new Text(neTag);
					beginPos = i;
				}
			}

			return new IntPair(beginPos, tokens.getNumTokens() - beginPos);
		}

		// TODO: there has to be a better way...
		private int getOffset(Text text, String sentence) {
			String paddedText = " " + text + " ";

			int offset = 0;
			for(int i = 0; i < sentence.length() - text.getLength(); i++) {
				if(sentence.charAt(i) == ' ') {
					offset++;
				}
				
				if(i == 0) {
					paddedText = text + " ";
				}
				else if(i == 1) {
					paddedText = " " + text + " ";
				}
				else if(i == sentence.length() - text.getLength() - 1) {
					paddedText = " " + text;
				}
				
				if(sentence.regionMatches(i, paddedText, 0, paddedText.length())) {
					return offset;
				}
			}
			return -1;
		}

		private int getLength(Text text) {
			int length = 1;
			byte [] b = text.getBytes();
			for(int i = 0; i < text.getLength(); i++) {
				if(b[i] == ' ') {
					length++;
				}
			}
			return length;
		}

		private Set<Text> getTypes(List<SentenceWritable<TratzParsedTokenWritable>> allSentences, SentenceWritable<TratzParsedTokenWritable> curSentence, int sentId, int offset, int length, boolean useCoref) {
			Set<Text> types = new HashSet<Text>();
			types.add(DEFAULT_TYPE);

			// get user-specified types (if specified)
			Set<IdWeightPair> instanceClasses = getMatchingInstances(curSentence, offset, length);
			if(instanceClasses != null) {
				for(IdWeightPair instanceClass : instanceClasses) {
					//System.out.println("Instance is of type: " + instanceClass.id);
					types.add(instanceClass.id);
				}
			}

			// get NE types
			for(int i = offset; i < offset + length; i++) {
				Text type = curSentence.getTokenAt(i).getNETag();
				//System.out.println(curSentence.getTokenAt(i).getToken() + " is of type: " + type);
				types.add(type);
			}

			//			// get types (via coref)
			//			if(useCoref) {
			//				for(int i = offset; i < offset + length; i++) {
			//					IntPair pair = new IntPair(sentId, i);
			//					Integer corefId = mCorefForwardLookup.get(pair);
			//					if(corefId != null) {
			//						List<IntPair> pairs = mCorefReverseLookup.get(corefId);
			//						for(IntPair p : pairs) {
			//							Text type = allSentences.get(p.getSource()).getTokenAt(p.getTarget()).getNETag();
			//							//System.out.println(curSentence.getTokenAt(i).getToken() + " is of type: " + type + " [via coref!]");
			//							types.add(type);
			//
			//							instanceClasses = getMatchingInstances(allSentences.get(p.getSource()), p.getTarget(), 1);
			//							for(IdWeightPair instanceClass : instanceClasses) {
			//								//System.out.println("Instance is of type: " + instanceClass.id + " [via coref!]");
			//								types.add(instanceClass.id);
			//							}
			//						}
			//					}
			//				}
			//			}

			return types;
		}

		private Set<IdWeightPair> getMatchingInstances(SentenceWritable<TratzParsedTokenWritable> sentence, int offset, int length) {
			if(mInstances.size() == 0) {
				return null;
			}

			// get tokens
			Text [] tokens = new Text[length];
			for(int i = offset; i < offset + length; i++) {
				tokens[i-offset] = new Text(sentence.getTokenAt(i).getToken().toString());
			}
			
			Set<IdWeightPair> matches = new HashSet<IdWeightPair>();

			Text pattern = new Text();
			for(int s = 0; s < length; s++) {
				for(int l = 1; l < length; l++) {
					if(s + l > length) {
						continue;
					}

					pattern.clear(); 
					for(int pos = s; pos < s + l; pos++) {
						pattern.append(tokens[pos].getBytes(), 0, tokens[pos].getLength());
						if(pos != s + l - 1) {
							pattern.append(MavunoUtils.SPACE_BYTES, 0, MavunoUtils.SPACE_BYTES_LENGTH);
						}
					}

					List<IdWeightPair> list = mInstances.get(pattern);
					if(list != null) {
						//System.out.println("INSTANCE FOUND: " + pattern);
						matches.addAll(list);
					}
				}
			}

			return matches;
		}

	}

	private static class MyReducer extends Reducer<Text, Text, Text, Text> {

		@Override
		public void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			String outputPath = MavunoUtils.getRequiredParam("Mavuno.ExtractRelations.OutputPath", conf);
			BufferedWriter writer = MavunoUtils.getBufferedWriter(conf, outputPath + "/" + key + ".xml");
			for(Text value : values) {
				writer.write(value.toString());
			}
			writer.close();
		}
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.util.Tool#run(java.lang.String[])
	 */
	@Override
	public int run(String[] args) throws ClassNotFoundException, InterruptedException, IOException {
		MavunoUtils.readParameters(args, "Mavuno.ExtractRelations", getConf());
		return run();
	}

	public int run() throws ClassNotFoundException, InterruptedException, IOException {
		Configuration conf = getConf();

		String typesPath = MavunoUtils.getRequiredParam("Mavuno.ExtractRelations.TypesPath", conf);
		String primaryTypes = MavunoUtils.getRequiredParam("Mavuno.ExtractRelations.PrimaryTypes", conf);
		String patternsPath = MavunoUtils.getRequiredParam("Mavuno.ExtractRelations.PatternsPath", conf);
		String instancesPath = MavunoUtils.getOptionalParam("Mavuno.ExtractRelations.InstancesPath", conf);
		String plaintextPath = MavunoUtils.getOptionalParam("Mavuno.ExtractRelations.PlaintextPath", conf);
		String corpusPath = MavunoUtils.getRequiredParam("Mavuno.ExtractRelations.CorpusPath", conf);
		String extractorClass = MavunoUtils.getRequiredParam("Mavuno.ExtractRelations.ExtractorClass", conf);
		String extractorArgs = MavunoUtils.getRequiredParam("Mavuno.ExtractRelations.ExtractorArgs", conf);
		String outputPath = MavunoUtils.getRequiredParam("Mavuno.ExtractRelations.OutputPath", conf);

		sLogger.info("Tool name: ExtractRelations");
		sLogger.info(" - Types path: " + typesPath);
		sLogger.info(" - Primary types: " + primaryTypes);
		sLogger.info(" - Patterns path: " + patternsPath);
		if(instancesPath != null) {
			sLogger.info(" - Instances path: " + instancesPath);
		}
		if(plaintextPath != null) {
			sLogger.info(" - Plaintext path: " + plaintextPath);
		}
		sLogger.info(" - Corpus path: " + corpusPath);
		sLogger.info(" - Extractor class: " + extractorClass);
		sLogger.info(" - Extractor arguments: " + extractorArgs);
		sLogger.info(" - Output path: " + outputPath);

		Job job = new Job(conf);
		job.setJobName("ExtractRelations");

		FileInputFormat.addInputPath(job, new Path(corpusPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));

		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);

		job.waitForCompletion(true);
		return 0;
	}

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		int res = ToolRunner.run(new ExtractRelations(conf), args);
		System.exit(res);
	}
}

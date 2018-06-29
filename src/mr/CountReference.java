package mr;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class CountReference {

	public static class PageKey implements WritableComparable<PageKey> {
		String pageTo;
		String pageFrom;

		public PageKey() {
		}

		public PageKey(String pageTo, String pageFrom) {
			super();
			this.pageTo = pageTo;
			this.pageFrom = pageFrom;
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			pageTo = WritableUtils.readString(in);
			pageFrom = WritableUtils.readString(in);
		}

		@Override
		public void write(DataOutput out) throws IOException {
			WritableUtils.writeString(out, pageTo);
			WritableUtils.writeString(out, pageFrom);
		}

		@Override
		public int compareTo(PageKey o) {
			int cmp = pageTo.compareTo(o.pageTo);
			if (cmp == 0)
				cmp = pageFrom.compareTo(o.pageFrom);
			return cmp;
		}

		@Override
		public String toString() {
			return pageTo + "|" + pageFrom;
		}

	}

	/**
	 * Adapted from Apache/Mahout XML Input Format
	 * 
	 * Project Repo: https://github.com/apache/mahout
	 * 
	 * XmlInputFormat:
	 * https://github.com/apache/mahout/blob/574ccc990673afdd34cb47f2b50f4379c5823212/integration/src/main/java/org/apache/mahout/text/wikipedia/XmlInputFormat.java
	 * 
	 * Changes applied: - remove dependency to "com.google.common.io.Closeables" and
	 * "org.apache.commons.io.Charsets"
	 */
	public static class XmlInputFormat extends TextInputFormat {
		public static final String START_TAG_KEY = "xmlinput.start";
		public static final String END_TAG_KEY = "xmlinput.end";

		@Override
		public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context) {
			try {
				return new XmlRecordReader((FileSplit) split, context.getConfiguration());
			} catch (IOException ioe) {
				ioe.printStackTrace();
				return null;
			}
		}

		/**
		 * XMLRecordReader class to read through a given xml document to output xml
		 * blocks as records as specified by the start tag and end tag
		 *
		 */
		public static class XmlRecordReader extends RecordReader<LongWritable, Text> {
			private byte[] startTag;
			private byte[] endTag;
			private long start;
			private long end;
			private FSDataInputStream fsin;
			private DataOutputBuffer buffer = new DataOutputBuffer();
			private LongWritable currentKey = new LongWritable();
			private Text currentValue = new Text();

			public XmlRecordReader(FileSplit split, Configuration conf) throws IOException {
				startTag = conf.get(START_TAG_KEY).getBytes("UTF-8");
				endTag = conf.get(END_TAG_KEY).getBytes("UTF-8");

				// open the file and seek to the start of the split
				start = split.getStart();
				end = start + split.getLength();
				Path file = split.getPath();
				FileSystem fs = file.getFileSystem(conf);
				fsin = fs.open(split.getPath());
				fsin.seek(start);
			}

			@Override
			public void initialize(InputSplit split, TaskAttemptContext context)
					throws IOException, InterruptedException {
			}

			@Override
			public boolean nextKeyValue() throws IOException, InterruptedException {
				if (fsin.getPos() < end) {
					if (readUntilMatch(startTag, false)) {
						try {
							buffer.write(startTag);
							if (readUntilMatch(endTag, true)) {
								currentKey.set(fsin.getPos());
								currentValue.set(buffer.getData(), 0, buffer.getLength());
								return true;
							}
						} finally {
							buffer.reset();
						}
					}
				}
				return false;
			}

			@Override
			public LongWritable getCurrentKey() throws IOException, InterruptedException {
				return currentKey;
			}

			@Override
			public Text getCurrentValue() throws IOException, InterruptedException {
				return currentValue;
			}

			@Override
			public void close() throws IOException {
				fsin.close();
			}

			@Override
			public float getProgress() throws IOException {
				return (fsin.getPos() - start) / (float) (end - start);
			}

			private boolean readUntilMatch(byte[] match, boolean withinBlock) throws IOException {
				int i = 0;
				while (true) {
					int b = fsin.read();
					// end of file:
					if (b == -1)
						return false;
					// save to buffer:
					if (withinBlock)
						buffer.write(b);
					// check if we're matching:
					if (b == match[i]) {
						i++;
						if (i >= match.length)
							return true;
					} else
						i = 0;
					// see if we've passed the stop point:
					if (!withinBlock && i == 0 && fsin.getPos() >= end)
						return false;
				}
			}
		}
	}

	private static IntWritable one = new IntWritable(1);

	public static class Mapper1 extends Mapper<LongWritable, Text, PageKey, IntWritable> {
		private static String regex = "\\[\\[(.*?)\\]\\]";
		private static Pattern pattern = Pattern.compile(regex);

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String document = value.toString();
			try {
				XMLStreamReader reader = XMLInputFactory.newInstance()
						.createXMLStreamReader(new ByteArrayInputStream(document.getBytes("UTF-8")));
				String recordKey = "";
				String recordValue = "";
				String currentElement = "";
				while (reader.hasNext()) {
					int code = reader.next();
					switch (code) {
					case XMLStreamConstants.START_ELEMENT: // START_ELEMENT:
						currentElement = reader.getLocalName();
						break;
					case XMLStreamConstants.CHARACTERS: // CHARACTERS:

						if (currentElement.equalsIgnoreCase("text")) {
							recordValue += reader.getText();
						} else if (currentElement.equalsIgnoreCase("title")) {
							recordKey += reader.getText();
						}
						break;
					}
				}
				reader.close();
				if (!recordKey.equals("") && !recordValue.equals(""))
					parseText(recordKey, recordValue, context);

			} catch (Exception e) {

				throw new IOException(e);
			}
		}

		private void parseText(String title, String text, Context context) throws IOException, InterruptedException {
			Matcher matcher = pattern.matcher(text);
			while (matcher.find()) {
				String reference = matcher.group(0);
				if (reference != null && !reference.equals("")) {
					if (reference.contains("|") && !(reference.startsWith("|") && !reference.startsWith(" |"))) {
						reference = reference.split("\\|")[0];
					}
					if (reference.contains("File:") || reference.contains("Categoria:") || reference.contains("Aiuto:")
							|| reference.contains("s:"))
						continue;
				} else {
					continue;
				}
				reference = reference.replaceAll("\\[|\\]|\\,", "").trim();
				if (reference.equals("") == false) {
					PageKey pk = new PageKey(reference, title.toString().trim());
					context.write(pk, one);
				}
			}
		}
	}

	public static class Reducer1 extends Reducer<PageKey, IntWritable, Text, IntWritable> {

		@Override
		protected void reduce(PageKey key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			context.write(new Text(key.pageTo /* + "|" + key.pageFrom */), one);
		}
	}

	public static class Mapper2 extends Mapper<LongWritable, Text, Text, IntWritable> {

		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			String[] tok = value.toString().split("\t");
			context.write(new Text(tok[0]), new IntWritable(Integer.parseInt(tok[1])));
		}

	}

	public static class Reducer2 extends Reducer<Text, IntWritable, NullWritable, Text> {

		@Override
		protected void setup(Reducer<Text, IntWritable, NullWritable, Text>.Context context)
				throws IOException, InterruptedException {
			context.write(NullWritable.get(), new Text("page_title,count"));
		}

		@Override
		protected void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable i : values) {
				sum += i.get();
			}
			context.write(NullWritable.get(), new Text(key.toString() + "," + sum));
		}
	}

	public static class PageKeyGroupComparator extends WritableComparator {

		protected PageKeyGroupComparator() {
			super(PageKey.class, true);
		}

		@Override
		public int compare(WritableComparable a, WritableComparable b) {
			PageKey ac = (PageKey) a;
			PageKey bc = (PageKey) b;
			return ac.compareTo(bc);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set("xmlinput.start", "<page>");
		conf.set("xmlinput.end", "</page>");
		Job job = Job.getInstance(conf, "Count Reference");
		job.setJarByClass(CountReference.class);
		job.setOutputKeyClass(PageKey.class);
		job.setOutputValueClass(IntWritable.class);
		job.setMapperClass(Mapper1.class);
		job.setGroupingComparatorClass(PageKeyGroupComparator.class);
		job.setReducerClass(Reducer1.class);

		job.setInputFormatClass(XmlInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		boolean success = job.waitForCompletion(true);

		if (success) {
			Job job2 = Job.getInstance(conf, "Job2");

			job2.setMapperClass(Mapper2.class);

			job2.setReducerClass(Reducer2.class);

			job2.setOutputKeyClass(Text.class);
			job2.setOutputValueClass(IntWritable.class);
			job2.setJarByClass(CountReference.class);

			FileInputFormat.addInputPath(job2, new Path(args[1]));
			FileOutputFormat.setOutputPath(job2, new Path(args[1] + "/1"));

			success = job2.waitForCompletion(true);
		}

		if (success) {
			createCSV(args[1]);
			System.exit(0);
		} else
			System.exit(1);
	}

	private static void createCSV(String basePath) {
		File source = new File(basePath+"/1/part-r-00000");
		File dest = new File("result/incomingReferencesCount.csv");

		try {
			Files.copy(source.toPath(), dest.toPath(), StandardCopyOption.REPLACE_EXISTING);
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		System.out.println(" == CSV CREATED == ");

	}
}

package mr;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class CountReference {
	private static IntWritable one = new IntWritable(1);

	public static class Mapper1 extends Mapper<LongWritable, Text, PageKey, IntWritable> {
		private static String regex = "\\[\\[(.*?)\\]\\]";
		private static Pattern pattern = Pattern.compile(regex);

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String document = value.toString();
			// System.out.println(document);
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
				String reference = matcher.group(1);
				if (reference.contains("|")) {
					reference = reference.split("\\|")[0];
				}
				if(reference.contains("File:") || reference.contains("Categoria:") || reference.contains("Aiuto:") || reference.contains("s:"))
					continue;
				reference = reference.replaceAll("\\(|\\)|,|;|:", "");
				PageKey pk = new PageKey(reference.trim(), title.toString().trim());
				context.write(pk, one);
			}
		}
	}

	public static class Reducer1 extends Reducer<PageKey, IntWritable, Text, IntWritable> {

		@Override
		protected void reduce(PageKey key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			context.write(new Text(key.pageTo), one);
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
		
		job.setOutputKeyClass(PageKey.class);
		job.setOutputValueClass(IntWritable.class);
		job.setMapperClass(Mapper1.class);
//		job.setGroupingComparatorClass(PageKeyGroupComparator.class);
		job.setReducerClass(Reducer1.class);

		job.setInputFormatClass(XmlInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}

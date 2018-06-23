package mr;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class CountReference {

	public static class Mapper1 extends Mapper<LongWritable, Text, Text, Text> {

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
				context.write(new Text(recordKey.trim()), new Text(recordValue.trim()));

			} catch (Exception e) {
				throw new IOException(e);
			}
		}
	}

	public static class Reducer1 extends Reducer<Text, Text, Text, Text> {

		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			for (Text t : values) {
				context.write(new Text("KEY:" + key.toString()), new Text("TEXT:" + t));
			}
		}

	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set("xmlinput.start", "<page>");
		conf.set("xmlinput.end", "</page>");
		Job job = Job.getInstance(conf, "Count Reference");
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setMapperClass(Mapper1.class);
		job.setReducerClass(Reducer1.class);

		job.setInputFormatClass(XmlInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}

package com.ilab.transform;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.cloud.dataflow.sdk.options.GcsOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.util.GcsUtil;
import com.google.cloud.dataflow.sdk.util.gcsfs.GcsPath;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.ilab.loganalysis.model.Log;
import com.ilab.loganalysis.model.LogWithMessage;
import com.ilab.loganalysis.model.Message;
import com.ilab.loganalysis.model.StringWithMessage;

public class FetchLog extends PTransform<PCollection<Message>, PCollection<LogWithMessage>> {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public PCollection<LogWithMessage> apply(PCollection<Message> messages) {
		PCollection<StringWithMessage> urls = messages.apply(ParDo.of(new MessageFormatAsUrlFn()));
		PCollection<StringWithMessage> lines = urls.apply(ParDo.of(new UrlMapToLineFn()));
		PCollection<LogWithMessage> logs = lines.apply(ParDo.of(new LineFormatAsLogFn()));

		return logs;
	}
	
	private static String replaceLast(String string, String from, String to) {
		int lastIndex = string.lastIndexOf(from);
		if (lastIndex < 0)
			return string;
		String tail = string.substring(lastIndex).replaceFirst(from, to);
		return string.substring(0, lastIndex) + tail;
	}
	
	private static class MessageFormatAsUrlFn extends DoFn<Message, StringWithMessage> {
		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		@Override
		public void processElement(ProcessContext c) throws IOException {
			Message message = c.element();

			String gcsuri = "gs://" + message.getBucketName() + "/" + message.getObjectName();

			c.output(new StringWithMessage(gcsuri, message));
		}
	}

	private static class UrlMapToLineFn extends DoFn<StringWithMessage, StringWithMessage> {
		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		@Override
		public void processElement(ProcessContext c) throws IOException {
			StringWithMessage urlWithMessage = c.element();

			GcsPath path = GcsPath.fromUri(urlWithMessage.getData());
			GcsOptions options = PipelineOptionsFactory.as(GcsOptions.class);
			GcsUtil util = options.getGcsUtil();
			
			ReadableByteChannel rc = null;
			BufferedReader bf = null;

			try {
				rc = util.open(path);
				bf = new BufferedReader(Channels.newReader(rc, "UTF8"));

				String line;

				while ((line = bf.readLine()) != null) {
					c.output(new StringWithMessage(line, urlWithMessage.getMessage()));
				}
			} catch (IOException e) {
				//
			} finally {
				if (bf != null) {
					bf.close();
					bf = null;
				}
			}
		}
	}

	private static class LineFormatAsLogFn extends DoFn<StringWithMessage, LogWithMessage> {
		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;
		private static final String IPV4_REGEX = "^(([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\.){3}([01]?\\d\\d?|2[0-4]\\d|25[0-5])$";
		private transient Pattern pattern;
		
		@Override
	    public void startBundle(Context c) {
			pattern = Pattern.compile(IPV4_REGEX);
	    }
		
		@Override
		public void processElement(ProcessContext c) {
			StringWithMessage lineWithMessage = c.element();

			String line = lineWithMessage.getData();

			line = line.replaceFirst("\"", "");
			line = replaceLast(line, "\"", "");

			String[] datas = line.split("\",\"");

			if (datas.length == 17) {
				Log log = new Log();
				log.setTimeMicros(datas[0]);
				log.setcIp(datas[1]);
				log.setcIpType(datas[2]);
				log.setcIpRegion(datas[3]);
				log.setCsMethod(datas[4]);
				log.setCs_uri(datas[5]);
				log.setScStatus(datas[6]);
				log.setScBytes(datas[7]);
				log.setCsBytes(datas[8]);
				log.setTimeTakenMicros(datas[9]);
				log.setCsHost(datas[10]);
				log.setCsReferer(datas[11]);
				log.setCsUserAgent(datas[12]);
				log.setsRequestId(datas[13]);
				log.setCsOperation(datas[14]);
				log.setCsBucket(datas[15]);
				log.setCsObject(datas[16]);
				
				Matcher matcher = pattern.matcher(log.getcIp());
				if(matcher.matches()){
					c.output(new LogWithMessage(log, lineWithMessage.getMessage()));
				}
			}
		}
	}
}

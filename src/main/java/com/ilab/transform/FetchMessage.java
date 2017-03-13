package com.ilab.transform;

import java.io.StringReader;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonReader;

import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.ilab.loganalysis.model.Message;

public class FetchMessage extends PTransform<PCollection<String>, PCollection<Message>> {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public PCollection<Message> apply(PCollection<String> lines) {
		PCollection<Message> messages = lines.apply(ParDo.of(new JsonFormatAsMessageFn()));

		return messages;
	}
	
	private static class JsonFormatAsMessageFn extends DoFn<String, Message> {
		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		@Override
		public void processElement(ProcessContext c) {
			JsonReader reader = Json.createReader(new StringReader(c.element()));
			JsonObject json = reader.readObject();
			String bucket = json.getString("bucket");
			String objectName = json.getString("objectName");

			if (objectName.startsWith("vieshow_usage") || objectName.startsWith("vieshowtest_usage") ) {
				Message message = new Message();
				message.setBucketName(bucket);
				message.setObjectName(objectName);

				c.output(message);
			}
		}
	}
}

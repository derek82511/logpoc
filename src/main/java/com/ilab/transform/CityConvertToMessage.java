package com.ilab.transform;

import javax.json.Json;

import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.PCollection;

public class CityConvertToMessage extends PTransform<PCollection<String>, PCollection<String>> {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public PCollection<String> apply(PCollection<String> cities) {
		return cities.apply(ParDo.of(new CityFormatAsJsonString()));
	}
	
	private static class CityFormatAsJsonString extends DoFn<String, String> {
		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		@Override
		public void processElement(ProcessContext c) {
			String jsonString = Json.createObjectBuilder().add("city", c.element()).build().toString();

			c.output(jsonString);
		}
	}
}

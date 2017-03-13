package com.ilab.transform;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.net.HttpURLConnection;
import java.net.URL;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonReader;

import com.google.cloud.dataflow.sdk.transforms.Count;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.ilab.loganalysis.model.LogWithMessage;

public class LogToCityCount extends PTransform<PCollection<LogWithMessage>, PCollection<KV<String, Long>>> {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public PCollection<KV<String, Long>> apply(PCollection<LogWithMessage> logs) {
		PCollection<String> cities = logs.apply(ParDo.of(new LogFormatAsCity()));
		PCollection<KV<String, Long>> cityCounts = cities.apply(Count.<String> perElement());

		return cityCounts;
	}
	
	private static JsonObject getLocationFromIp(String ip) throws Exception{
		String responseStr = null;
		
		BufferedReader bf = null;
		try {
			URL url = new URL("http://geoip.nekudo.com/api/" + ip + "/en");
			HttpURLConnection con = (HttpURLConnection) url.openConnection();
			
			con.setRequestMethod("GET");

			bf = new BufferedReader(new InputStreamReader(con.getInputStream()));
			
			String inputLine;
			StringBuffer response = new StringBuffer();

			while ((inputLine = bf.readLine()) != null) {
				response.append(inputLine);
			}
			
			responseStr = response.toString();
		} catch (IOException e) {
			//
		} finally {
			if (bf != null) {
				bf.close();
				bf = null;
			}
		}
		
		if (responseStr != null) {
			try {
				JsonReader reader = Json.createReader(new StringReader(responseStr));
				JsonObject json = reader.readObject();
				
				return json;
			} catch (Exception e) {
				throw e;
			}
		} else {
			throw new Exception("no response");
		}
	}
	
	private static JsonObject getMapDataFromLocation(String latitude, String longitude) throws Exception{
		String responseStr = null;
		
		BufferedReader bf = null;
		
		try {
			URL url = new URL("http://maps.googleapis.com/maps/api/geocode/json?latlng=" + latitude + "," + longitude + "&language=en");
			HttpURLConnection con = (HttpURLConnection) url.openConnection();
			
			con.setRequestMethod("GET");

			bf = new BufferedReader(new InputStreamReader(con.getInputStream()));
			
			String inputLine;
			StringBuffer response = new StringBuffer();

			while ((inputLine = bf.readLine()) != null) {
				response.append(inputLine);
			}
			
			responseStr = response.toString();
		} catch (IOException e) {
			//
		} finally {
			if (bf != null) {
				bf.close();
				bf = null;
			}
		}
		
		if (responseStr != null) {
			try {
				JsonReader reader = Json.createReader(new StringReader(responseStr));
				JsonObject json = reader.readObject();
				String status = json.getString("status");
				
				if(!status.equals("OK")){
					throw new Exception("response failed : " + status);
				}
				
				return json;
			} catch (Exception e) {
				throw e;
			}
		} else {
			throw new Exception("no response");
		}
	}
	
	private static String getCityFromMapData(JsonObject mapData) throws Exception{
		try {
			JsonArray results = mapData.getJsonArray("results");
			for(int i = 0; i < results.size(); i++){
				JsonObject result = results.getJsonObject(i);
				JsonArray address_components = result.getJsonArray("address_components");
				
				for(int j = 0; j < address_components.size(); j++){
					JsonObject address_component = address_components.getJsonObject(j);
					JsonArray types = address_component.getJsonArray("types");
					
					for(int k = 0; k < types.size(); k++){
						String type = types.getString(k);
						if(type.equals("locality")){
							return address_component.getString("long_name");
						}
					}
				}
			}
			throw new Exception("locality not found");
		} catch (Exception e) {
			throw e;
		}
	}
	
	private static class LogFormatAsCity extends DoFn<LogWithMessage, String> {
		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		@Override
		public void processElement(ProcessContext c) throws Exception {
			String ip = c.element().getData().getcIp();
			
			try {
				JsonObject locationJson = getLocationFromIp(ip);
				
				JsonObject location = locationJson.getJsonObject("location");
				String latitude = location.getJsonNumber("latitude").toString();
				String longitude = location.getJsonNumber("longitude").toString();
				
				JsonObject mapData = getMapDataFromLocation(latitude, longitude);
				
				String city = getCityFromMapData(mapData);
				
				c.output(city);
			} catch (Exception e) {
				//
			}
		}
	}
}

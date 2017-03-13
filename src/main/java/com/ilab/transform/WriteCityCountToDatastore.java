package com.ilab.transform;

import static com.google.datastore.v1.client.DatastoreHelper.makeKey;
import static com.google.datastore.v1.client.DatastoreHelper.makeUpsert;
import static com.google.datastore.v1.client.DatastoreHelper.makeValue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.joda.time.Duration;

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.util.BackOff;
import com.google.api.client.util.BackOffUtils;
import com.google.api.client.util.Sleeper;
import com.google.cloud.dataflow.sdk.options.GcpOptions;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.util.FluentBackoff;
import com.google.cloud.dataflow.sdk.util.RetryHttpRequestInitializer;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.datastore.v1.CommitRequest;
import com.google.datastore.v1.Entity;
import com.google.datastore.v1.Key;
import com.google.datastore.v1.Mutation;
import com.google.datastore.v1.Value;
import com.google.datastore.v1.client.Datastore;
import com.google.datastore.v1.client.DatastoreException;
import com.google.datastore.v1.client.DatastoreFactory;
import com.google.datastore.v1.client.DatastoreOptions;

public class WriteCityCountToDatastore extends PTransform<PCollection<KV<String, Long>>, PCollection<String>> {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private final String projectId;
	
	public WriteCityCountToDatastore(String projectId) {
    	this.projectId = projectId;
    }
	
	@Override
	public PCollection<String> apply(PCollection<KV<String, Long>> cityCounts) {
		PCollection<String> reducedCities = cityCounts.apply(ParDo.of(new WriteCityCountFn(projectId)));

		return reducedCities;
	}
	
	private static class WriteCityCountFn extends DoFn<KV<String, Long>, String> {
		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;
		private final String projectId;
	    private transient Datastore datastore;
	    private final List<Mutation> mutations = new ArrayList<>();
		
	    private static final int DATASTORE_BATCH_UPDATE_LIMIT = 500;
	    private static final int MAX_RETRIES = 5;
	    private static final FluentBackoff BUNDLE_WRITE_BACKOFF = FluentBackoff.DEFAULT.withMaxRetries(MAX_RETRIES).withInitialBackoff(Duration.standardSeconds(5));
	    
	    public WriteCityCountFn(String projectId) {
	    	this.projectId = projectId;
	    }
	    
	    @Override
	    public void startBundle(Context c) {
	    	DatastoreOptions.Builder builder = new DatastoreOptions.Builder().projectId(projectId)
					.initializer(new RetryHttpRequestInitializer());

			Credential credential = c.getPipelineOptions().as(GcpOptions.class).getGcpCredential();
			if (credential != null) {
				builder.credential(credential);
			}

			datastore = DatastoreFactory.get().create(builder.build());
	    }
		
		@Override
		public void processElement(ProcessContext c) throws Exception {
			KV<String, Long> cityCount = c.element();

			Entity.Builder entityBuilder = Entity.newBuilder();
			Key.Builder keyBuilder = makeKey("cityCount", UUID.randomUUID().toString());

			entityBuilder.setKey(keyBuilder.build());

			Map<String, Value> values = entityBuilder.getMutableProperties();
			values.put("city", makeValue(cityCount.getKey()).build());
			values.put("count", makeValue(cityCount.getValue()).build());
			
			mutations.add(makeUpsert(entityBuilder.build()).build());
			
			if (mutations.size() >= DATASTORE_BATCH_UPDATE_LIMIT) {
				flushBatch();
		    }

			c.output(cityCount.getKey());
		}
		
		@Override
	    public void finishBundle(Context c) throws Exception {
			if (!mutations.isEmpty()) {
				flushBatch();
			}
	    }
		
		private void flushBatch() throws DatastoreException, IOException, InterruptedException {
			Sleeper sleeper = Sleeper.DEFAULT;
			BackOff backoff = BUNDLE_WRITE_BACKOFF.backoff();
		    
		    while (true) {
		        try {
		        	CommitRequest.Builder commitRequest = CommitRequest.newBuilder();
		        	commitRequest.addAllMutations(mutations);
		        	commitRequest.setMode(CommitRequest.Mode.NON_TRANSACTIONAL);
		        	datastore.commit(commitRequest.build());
		        	
		        	break;
		        } catch (DatastoreException exception) {
		        	if (!BackOffUtils.next(sleeper, backoff)) {
		        		throw exception;
		        	}
		        }
		    }
		    mutations.clear();
		}
	}
}
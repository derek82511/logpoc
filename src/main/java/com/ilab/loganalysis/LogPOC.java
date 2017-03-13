package com.ilab.loganalysis;

import java.io.IOException;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.joda.time.Duration;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.PipelineResult;
import com.google.cloud.dataflow.sdk.coders.StringUtf8Coder;
import com.google.cloud.dataflow.sdk.io.PubsubIO;
import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions;
import com.google.cloud.dataflow.sdk.options.Default;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.runners.DataflowPipelineJob;
import com.google.cloud.dataflow.sdk.runners.DataflowPipelineRunner;
import com.google.cloud.dataflow.sdk.util.MonitoringUtil;
import com.google.cloud.dataflow.sdk.transforms.windowing.FixedWindows;
import com.google.cloud.dataflow.sdk.transforms.windowing.Window;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Uninterruptibles;
import com.ilab.loganalysis.model.LogWithMessage;
import com.ilab.loganalysis.model.Message;
import com.ilab.transform.CityConvertToMessage;
import com.ilab.transform.FetchLog;
import com.ilab.transform.FetchMessage;
import com.ilab.transform.LogToCityCount;
import com.ilab.transform.WriteCityCountToDatastore;

public class LogPOC {
	private static Set<DataflowPipelineJob> jobsToCancel = Sets.newHashSet();

	private interface LogPOCOptions extends DataflowPipelineOptions {
		@Default.String("projects/loganalysispoc/subscriptions/logpoc-subscription")
		String getSubscription();

		void setSubscription(String value);
		
		@Default.String("projects/loganalysispoc/topics/processDone")
		String getPubTopic();

		void setPubTopic(String value);
	}

	public static void main(String[] args) throws IOException {
		LogPOCOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(LogPOCOptions.class);

		options.setMaxNumWorkers(2);
		options.setStreaming(true);
		options.setRunner(DataflowPipelineRunner.class);

		Pipeline pipeline = Pipeline.create(options);

		PCollection<String> input = pipeline
				.apply(PubsubIO.Read.subscription(options.getSubscription()));

		PCollection<String> windowInput = input
				.apply(Window.<String> into(FixedWindows.of(Duration.standardSeconds(10))));

		PCollection<Message> messages = windowInput.apply(new FetchMessage());

		PCollection<LogWithMessage> logs = messages.apply(new FetchLog());

		PCollection<KV<String, Long>> cityCounts = logs.apply(new LogToCityCount());

		PCollection<String> cities = cityCounts.apply(new WriteCityCountToDatastore(options.getProject()));

		PCollection<String> pubMessages = cities.apply(new CityConvertToMessage());

		pubMessages.apply(
				PubsubIO.Write.topic(options.getPubTopic()).withCoder(StringUtf8Coder.of()));

		PipelineResult result = pipeline.run();

		waitToFinish(result);
	}

	private static void waitToFinish(PipelineResult result) {
		if (result instanceof DataflowPipelineJob) {
			final DataflowPipelineJob job = (DataflowPipelineJob) result;
			jobsToCancel.add(job);
			addShutdownHook(jobsToCancel);
			try {
				job.waitToFinish(-1, TimeUnit.SECONDS, new MonitoringUtil.PrintHandler(System.out));
			} catch (Exception e) {
				throw new RuntimeException("Failed to wait for job to finish: " + job.getJobId());
			}
		} else {
			// Do nothing if the given PipelineResult doesn't support
			// waitToFinish(),
			// such as EvaluationResults returned by DirectPipelineRunner.
		}
	}

	private static void addShutdownHook(final Collection<DataflowPipelineJob> jobs) {
		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				for (DataflowPipelineJob job : jobs) {
					System.out.println("Canceling example pipeline: " + job.getJobId());
					try {
						job.cancel();
					} catch (IOException e) {
						System.out.println("Failed to cancel the job,"
								+ " please go to the Developers Console to cancel it manually");
						System.out.println(MonitoringUtil.getJobMonitoringPageURL(job.getProjectId(), job.getJobId()));
					}
				}

				for (DataflowPipelineJob job : jobs) {
					boolean cancellationVerified = false;
					for (int retryAttempts = 6; retryAttempts > 0; retryAttempts--) {
						if (job.getState().isTerminal()) {
							cancellationVerified = true;
							System.out.println("Canceled example pipeline: " + job.getJobId());
							break;
						} else {
							System.out.println("The example pipeline is still running. Verifying the cancellation.");
						}
						Uninterruptibles.sleepUninterruptibly(10, TimeUnit.SECONDS);
					}
					if (!cancellationVerified) {
						System.out.println("Failed to verify the cancellation for job: " + job.getJobId());
						System.out.println("Please go to the Developers Console to verify manually:");
						System.out.println(MonitoringUtil.getJobMonitoringPageURL(job.getProjectId(), job.getJobId()));
					}
				}
			}
		});
	}

}

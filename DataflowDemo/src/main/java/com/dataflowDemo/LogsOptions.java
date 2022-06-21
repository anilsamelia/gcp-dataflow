package com.dataflowDemo;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;

/**
 * BqUploadOptions class is user-define class that contains all the input
 * parameters.
 *
 * @author Anil.Kumar
 * @version 1.0
 * @since 2022-02-08
 */
public interface LogsOptions extends PipelineOptions {

	@Description("Source Project Name")
	@Default.String("mw-data-analytics-sandbox")
	String getProjectId();
	void setProjectId(String projectId);

	@Description("Output BQ table to write results to")
	@Default.String("mw-data-analytics-sandbox")
	String getSinkProjectId();
	void setSinkProjectId(String sinkProjectId);

	@Description("Output BQ table to write results to")
	@Default.String("people_2")
	String getSinkDatasetId();
	void setSinkDatasetId(String sinkDatasetId);

	@Description("Output BQ table to write results to")
	@Default.String("dataflow_test")
	String getSinkName();
	void setSinkName(String sinkName);
	
	@Description("Output BQ table to write results to")
	@Default.String("bigquery_dataset")
	String getResourceType();
	void setResourceType(String resourceType);
	
	@Description("Output BQ table to write results to")
	@Default.String("INFO")
	String getSeverity();
	void setSeverity(String severity);

	@Description("Output BQ table to write results to")
	@Default.String("2022-06-10T06:46:23Z")
	String getStartTimestamp();
	void setStartTimestamp(String startTimestamp);
	
	@Description("Output BQ table to write results to")
	@Default.String("2022-06-10T6:51:23Z")
	String getEndTimestamp();
	void setEndTimestamp(String endTimestamp);

}

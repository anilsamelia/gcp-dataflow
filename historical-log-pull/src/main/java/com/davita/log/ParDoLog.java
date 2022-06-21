package com.davita.log;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.beam.sdk.transforms.DoFn;
import org.json.JSONArray;
import org.json.JSONObject;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.gax.paging.Page;
import com.google.cloud.logging.LogEntry;
import com.google.cloud.logging.Logging;
import com.google.cloud.logging.LoggingOptions;
import com.google.cloud.logging.Payload;
import com.google.gson.Gson;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.GeneratedMessageV3;
import com.google.cloud.logging.Logging.EntryListOption;

public class ParDoLog extends DoFn<String, String> {

	private String projectId;
	private String resourceType;
	private String severity;
	private String sinkProjectId;
	private String sinkDatasetId;
	private String sinkName;

	static final String LOCAL_DIR = "temp_output";

	public ParDoLog(String projectId, String resourceType, String severity, String sinkProjectId, String sinkDatasetId,
			String sinkName) {
		this.projectId = projectId;
		this.resourceType = resourceType;
		this.severity = severity;
		this.sinkProjectId = sinkProjectId;
		this.sinkDatasetId = sinkDatasetId;
		this.sinkName = sinkName;
	}

	@ProcessElement
	public void processElement(@Element String timeRange, OutputReceiver<String> receiver) {
		System.out.println(timeRange);
		String dateRange[] = timeRange.split(",");
		String startdate = LogsPipeline.getDate(Long.parseLong(dateRange[0]));
		String enddate = LogsPipeline.getDate(Long.parseLong(dateRange[1]));
		Logging logging = LoggingOptions.getDefaultInstance().getService();
		String filter = queryFilter(resourceType, severity, startdate, enddate);

		Page<LogEntry> entries = logging.listLogEntries(EntryListOption.filter(filter));
		List<String> logEntryList = new ArrayList<String>();
		
		Iterable it = entries.getValues();
		
		System.out.println(it);
		
		System.exit(100);
		while (entries != null) {
			for (LogEntry logEntry : entries.iterateAll()) {
				System.out.println(logEntry.getHttpRequest());
				
				Payload.ProtoPayload f = logEntry.getPayload();
				
				
				Map map = f.getData().getAllFields();
				
				GeneratedMessageV3 v= f.getData();
				
				
				System.out.println(f.getData().getTypeUrl());
				
				//System.out.println(map);
				//System.out.println(new Gson().toJson(f.getData()));
				System.exit(100);

				JSONObject json = new JSONObject();
				json.put("logName", logEntry.getLogName());
				json.put("timestamp", logEntry.getTimestamp());
				JSONObject resource = new JSONObject();
				resource.put("type", logEntry.getResource().getType());
				resource.put("labels", logEntry.getResource().getLabels());
				json.put("resource", resource);
				logEntryList.add(json.toString());

			}
			entries = entries.getNextPage();
		}

		File folder = new File(LOCAL_DIR);
		folder.mkdir();
		String fileName = LOCAL_DIR + FileSystems.getDefault().getSeparator() + "day" + dateRange[0] + ".json";
		FileWriter file = null;
		try {
			file = new FileWriter(fileName);
			file.write(new Gson().toJson(logEntryList));
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				file.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	public String queryFilter(String resourceType, String severity, String startTimeStamp, String endTimeStamp) {
		String query = "resource.type=\"" + resourceType + "\" severity=\"" + severity + "\""
//		 + " protoPayload.metadata.tableDataRead.policyTags!=None AND "
				+ " timestamp >= \"" + startTimeStamp + "\" AND timestamp <= \"" + endTimeStamp + "\" ";
		System.out.println(query);
		return query;
	}

}

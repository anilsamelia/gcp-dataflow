package com.davita.log;

import java.sql.Time;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;

public class LogsPipeline {

	public static void runPipeLine(LogsOptions options) {
		Pipeline p = Pipeline.create(options);
		List<String> days = getDays(options.getStartTimestamp(), options.getEndTimestamp());
		ParDoLog pardo = new ParDoLog(options.getProjectId(), options.getResourceType(), options.getSeverity(),
				 options.getSinkProjectId(),
				options.getSinkDatasetId(), options.getSinkName());
		p.apply(Create.of(days)).setCoder(StringUtf8Coder.of()).apply(ParDo.of(pardo));
		p.run();

	}

	private static List<String> getDays(String startDate, String endDate) {
		Date enddate = getDate(endDate);
		Date startdate = getDate(startDate);
		Long diff = enddate.getTime() - startdate.getTime();
		int days = (int) (diff / (1000 * 60 * 60 * 24));
		List<String> daysRange = new ArrayList<String>();
		String tempDate = String.valueOf(getDate(startDate).getTime());
		while (days > 0) {
			String from = String.valueOf(tempDate);
			Long nextDay = null;
			if (days > 1) {
				nextDay = Long.parseLong(from) + 86400000; // 24 hours added in range
			} else {
				nextDay = getDate(endDate).getTime();
			}
			String todate = String.valueOf(nextDay);
			String dayRange = from + "," + todate;
			tempDate = todate;
			daysRange.add(dayRange);
			days--;
		}
		if(daysRange.size()==0) {
			String singleday=startdate.getTime()+","+enddate.getTime();
			daysRange.add(singleday);
		}
		
		return daysRange;
	}

	public static Date getDate(String date) {
		Date date1 = null;
		try {
			date1 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'").parse(date);
		} catch (ParseException e) {

			e.printStackTrace();
		}
		return date1;
	}

	public static String getDate(Long milisec) {
		SimpleDateFormat dateformat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
		Date result = new Date(milisec);
		return dateformat.format(result);

	}

	public static void main(String[] args) {
		 LogsOptions options =PipelineOptionsFactory.fromArgs(args).withValidation().as(LogsOptions.class);
		 runPipeLine(options);

	}
	

}
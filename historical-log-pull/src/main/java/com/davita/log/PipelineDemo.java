package com.davita.log;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;

import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;

public class PipelineDemo {

	public static void main(String[] args) {
		Pipeline pipeline = Pipeline.create();
		PCollection<String> output = pipeline.apply(TextIO.read().from("D:\\demo\\input.csv"));

		PCollection<String> outputs = output.apply(MapElements.via(new User()));

		outputs.apply(ParDo.of(new DoFn<String, String>() {

			@ProcessElement
			public void processElement(@Element String element, OutputReceiver<String> receiver) {
				System.out.println(element);
			}

		}));

		pipeline.run();
	}

}

package com.davita.log;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.beam.sdk.transforms.SimpleFunction;

public class User extends SimpleFunction<String, String> {
	
	@Override
	public String apply(String input) {
		String str[] = input.split(",");
		List<String> list = Stream.of(str).map(obj-> ","+obj.concat("123")).collect(Collectors.toList());
		
		return list.get(0);
	}
}

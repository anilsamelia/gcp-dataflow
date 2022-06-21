package com.dataflowDemo;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;

public class Employee implements Serializable {
	private String name;
	private String designation;
	private int salary;

	public Employee(String name, String des, int salary) {
		this.name = name;
		this.designation = des;
		this.salary = salary;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getDesignation() {
		return designation;
	}

	public void setDesignation(String designation) {
		this.designation = designation;
	}

	public int getSalary() {
		return salary;
	}

	public void setSalary(int salary) {
		this.salary = salary;
	}

	@Override
	public String toString() {
		return name + "," + designation + "," + salary;
	}

	public static void main(String[] args) {
		Pipeline p = Pipeline.create();
		PCollection<Employee> plist = p.apply(Create.of(getList()));
		PCollection<String> list = plist
				.apply(MapElements.into(TypeDescriptors.strings()).via((Employee emp) -> emp.toString()));
		list.apply(TextIO.write().to("D:\\demo\\customer.csv").withHeader("NAME,DESIGNATION,SALARY").withNumShards(1)
				.withSuffix(".csv"));
		p.run();

	}

	static List<Employee> getList() {
		List<Employee> lst = new ArrayList<Employee>();
		lst.add(new Employee("Bob", "SSE", 100000));
		lst.add(new Employee("Mike", "SE", 50000));
		lst.add(new Employee("Sid", "SSE", 100000));
		lst.add(new Employee("King", "AE", 200000));
		lst.add(new Employee("David", "SSE", 100000));
		lst.add(new Employee("Joe", "AE", 50000));
		return lst;
	}
}

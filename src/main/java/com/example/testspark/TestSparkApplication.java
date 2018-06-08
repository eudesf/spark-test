package com.example.testspark;

import javax.annotation.PreDestroy;

import org.apache.spark.sql.SparkSession;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
public class TestSparkApplication {

	public static final String SPARK_APP = "TestSparkApp";
	
	public static void main(String[] args) {
		SpringApplication.run(TestSparkApplication.class, args);
	}
	
	@Bean
	public SparkSession getSparkSession() {
		return SparkSession.builder().appName(SPARK_APP).master("local").getOrCreate();
	}
	
	
	
	@PreDestroy
	public void preDestroy() {
		getSparkSession().stop();
	}
}

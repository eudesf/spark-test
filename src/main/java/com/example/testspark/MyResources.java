package com.example.testspark;

import org.apache.spark.sql.SparkSession;
import org.springframework.web.context.annotation.ApplicationScope;

@ApplicationScope
public class MyResources {

	private SparkSession sparkSession;

	public SparkSession getSparkSession() {
		return sparkSession;
	}

	public void setSparkSession(SparkSession sparkSession) {
		this.sparkSession = sparkSession;
	}
	
	
	
}

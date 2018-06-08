package com.example.testspark;

import java.io.File;
import java.io.IOException;

import javax.inject.Inject;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.SparkSession;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.google.common.io.Files;

import scala.Tuple2;

@RestController
public class TestSparkController {
		
	@Inject
	private SparkSession sparkSession;
	
	@PostMapping(value="/processData")
	@ResponseBody
	public String processData(@RequestBody String data) throws IOException {
		File tempFile = File.createTempFile("tmp", "spark");
		Files.write(data.getBytes(), tempFile);
		
		JavaRDD<String> lines = sparkSession.read().textFile(tempFile.getAbsolutePath()).javaRDD();
		
		JavaRDD<String> aLines = lines.filter(a -> a.contains("A"));
		JavaRDD<String> bLines = lines.filter(b -> b.contains("B"));
		JavaRDD<String> cLines = lines.filter(c -> c.contains("C"));
		JavaRDD<String> dLines = lines.filter(d -> d.contains("D"));
		JavaRDD<String> eLines = lines.filter(e -> e.contains("E"));
		
		Function<Tuple2<String, String>, String> convertTuple2= t2 -> t2._1() + t2._2();
		
		JavaPairRDD<String, String> bResult = aLines.cartesian(bLines);
		JavaPairRDD<String, String> cResult = bResult.map(convertTuple2).cartesian(cLines);
		JavaPairRDD<String, String> dResult = aLines.cartesian(dLines);
		JavaPairRDD<String, String> eResult = aLines.cartesian(eLines);
		
		
		JavaRDD<String> bcdeResult = bResult.union(cResult).union(dResult).union(eResult).map(convertTuple2); 
		return String.join("\n", aLines.union(bcdeResult).collect());
			
	}
}

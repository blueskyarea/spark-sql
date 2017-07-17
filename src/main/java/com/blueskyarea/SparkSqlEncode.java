package com.blueskyarea;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;

import com.blueskyarea.entity.Member;

public class SparkSqlEncode implements Serializable {

	private static final long serialVersionUID = -7680986398871659761L;

	public static void main(String[] args) {
        // create SparkContext
    	SparkConf conf = new SparkConf().setAppName("SparkSqlEncode").setMaster("local[*]");
    	JavaSparkContext jsc = new JavaSparkContext(conf);
    	
    	// create SqlContext
    	SQLContext sqlCtx = new HiveContext(jsc);
    	
    	// create data
    	SparkSqlEncode sqlEncode = new SparkSqlEncode();
    	List<Member> memberList = sqlEncode.createData();
    	JavaRDD<Member> memberRdd = jsc.parallelize(memberList);
    	
    	// create encoder
    	Encoder<Member> memberEncoder = Encoders.bean(Member.class);
    	
    	// create DataFrame
    	DataFrame dataFrame = sqlCtx.createDataFrame(memberRdd, Member.class);
    	dataFrame.show();
    	
    	// parquet
    	dataFrame.write().parquet("member.parquet");
    	
    	// encode
    	JavaRDD<Member> memberRdd2 = sqlCtx.read().parquet("member.parquet").as(memberEncoder).rdd().toJavaRDD();
    	
    	for(Member member : memberRdd2.collect()) {
    		System.out.println(member.getName());
    	}
	}
	
	private List<Member> createData() {
		List<Member> memberList = new ArrayList<>();
		memberList.add(new Member("Angelina Jolie", "address1", "xxx-xxxx-xxxx", "mail1"));
		memberList.add(new Member("Jessica Marie Alba", "address2", "yyy-yyyy-yyyy", "mail2"));
		memberList.add(new Member("Emma Charlotte Duerre Watson", "address3", "zzz-zzzz-zzzz", "mail3"));
		return memberList;
	}
}

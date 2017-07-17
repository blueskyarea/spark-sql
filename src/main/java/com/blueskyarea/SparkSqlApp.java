package com.blueskyarea;

import static org.apache.spark.sql.functions.row_number;
import static org.apache.spark.sql.functions.desc;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.hive.HiveContext;

public class SparkSqlApp implements Serializable {

	private static final long serialVersionUID = 95156543049152826L;

	public static void main( String[] args ) {
        // create SparkContext
    	SparkConf conf = new SparkConf().setAppName("SparkSqlApp").setMaster("local[*]");
    	JavaSparkContext jsc = new JavaSparkContext(conf);
    	
    	// create SqlContext
    	SQLContext sqlCtx = new HiveContext(jsc);
    	
    	// create data
    	SparkSqlApp sqlApp = new SparkSqlApp();
    	List<RecordData> recordDataList = sqlApp.createData();
    	JavaRDD<RecordData> recordRdd = jsc.parallelize(recordDataList);
    	
    	// create DataFrame
    	DataFrame dataFrame = sqlCtx.createDataFrame(recordRdd, RecordData.class);
    	dataFrame.printSchema();
    	
    	// calculation
    	DataFrame orderByJapanese = sqlApp.sorting(dataFrame, "japanese");
    	orderByJapanese.show();
    	DataFrame orderByMathematics = sqlApp.sorting(dataFrame, "mathematics");
    	orderByMathematics.show();
    	DataFrame orderByEnglish = sqlApp.sorting(dataFrame, "english");
    	orderByEnglish.show();
    	DataFrame orderBySocial = sqlApp.sorting(dataFrame, "social");
    	orderBySocial.show();
    	DataFrame orderByScience = sqlApp.sorting(dataFrame, "science");
    	orderByScience.show();
    	
    	DataFrame cal = sqlApp.calcTotalScore(dataFrame, orderByJapanese, orderByMathematics, orderByEnglish, orderBySocial, orderByScience);
    	cal.show();
    }
    
    private List<RecordData> createData() {
    	Random rnd = new Random();
    	List<RecordData> salesDataList = new ArrayList<>();
    	salesDataList.add(new RecordData("Abe", rnd.nextInt(101), rnd.nextInt(101), rnd.nextInt(101), rnd.nextInt(101), rnd.nextInt(101)));
    	salesDataList.add(new RecordData("Bush", rnd.nextInt(101), rnd.nextInt(101), rnd.nextInt(101), rnd.nextInt(101), rnd.nextInt(101)));
    	salesDataList.add(new RecordData("Clinton", rnd.nextInt(101), rnd.nextInt(101), rnd.nextInt(101), rnd.nextInt(101), rnd.nextInt(101)));
    	salesDataList.add(new RecordData("Donald", rnd.nextInt(101), rnd.nextInt(101), rnd.nextInt(101), rnd.nextInt(101), rnd.nextInt(101)));
    	salesDataList.add(new RecordData("Edison", rnd.nextInt(101), rnd.nextInt(101), rnd.nextInt(101), rnd.nextInt(101), rnd.nextInt(101)));
    	return salesDataList;
    }
    
    private DataFrame sorting(DataFrame df, String orderElement1) {
    	return df.select(row_number().over(Window.partitionBy().orderBy(orderElement1)).alias("score"),
    			new Column("name"),
    			new Column(orderElement1));
    }
    
    private DataFrame calcTotalScore(DataFrame df, DataFrame df1, DataFrame df2, DataFrame df3, DataFrame df4, DataFrame df5) {
    	return df.alias("originalDf").join(df1.alias("jaDf"), "name").join(df2.alias("maDf"), "name").join(df3.alias("enDf"), "name").join(df4.alias("soDf"), "name").join(df5.alias("scDf"), "name").select(
    			new Column("name"),
    			(new Column("jaDf.score").plus(new Column("maDf.score").plus(new Column("enDf.score").plus(new Column("soDf.score").plus(new Column("scDf.score"))))).alias("totalScore")),
    			new Column("originalDf.japanese"),
    			new Column("originalDf.mathematics"),
    			new Column("originalDf.english"),
    			new Column("originalDf.social"),
    			new Column("originalDf.science")).orderBy(desc("totalScore"));
    }

    public class RecordData implements Serializable {
		private static final long serialVersionUID = -8262025927050735682L;
		private String name;
		private Integer japanese;
    	private Integer mathematics;
    	private Integer english;
    	private Integer social;
    	private Integer science;
    	
    	public RecordData(String name, Integer japanese, Integer mathematics, Integer english, Integer social, Integer science) {
    		this.name = name;
    		this.japanese = japanese;
    		this.mathematics = mathematics;
    		this.english = english;
    		this.social = social;
    		this.science = science;
    	}
    	
    	// getter method are mandatory for dataFrame working
    	public String getName() {
    		return name;
    	}
    	
    	public Integer getJapanese() {
    		return japanese;
    	}
    	
    	public Integer getMathematics() {
    		return mathematics;
    	}
    	
    	public Integer getEnglish() {
    		return english;
    	}
    	
    	public Integer getSocial() {
    		return social;
    	}
    	
    	public Integer getScience() {
    		return science;
    	}
    }
}

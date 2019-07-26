package com.qiyi.kpp.olap.spark.tmp;

import org.apache.spark.sql.*;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.types.DataTypes;

import java.util.Properties;

public class TestSparkWithColumn {

    /**
     * spark直接读取mysql
     */
    private static Dataset<Row> queryMySQLData(SparkSession spark) {
        Properties properties = new Properties();
        properties.put("user", "root");
        properties.put("password", 123456);
        properties.put("driver", "com.mysql.jdbc.Driver");
        // 可以写SQL语句查询数据结果
        return spark.read().jdbc(
                "jdbc:mysql://127.0.0.1:3306/test"
                , "(select id, name from tb_data) tsub",
                properties);
    }

    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("TestSparkWithColumn")
                .master("local[4]")
                .getOrCreate();


        Dataset<Row> inputData = queryMySQLData(spark);

        inputData.show(20, false);

        // 方法1：使用functions中的函数，有一些局限性
        inputData.withColumn("name_length_method1", functions.length(inputData.col("name")));

        // 方法2：自定义注册udf，可以用JAVA代码写处理
        spark.udf().register(
                "getLength",
                new UDF1<String, Integer>() {
                    @Override
                    public Integer call(String s) throws Exception {
                        return s.length();
                    }
                },
                DataTypes.IntegerType);

        inputData = inputData.withColumn(
                "name_length_method2",
                functions.callUDF("getLength",
                        inputData.col("name"))
        );

        // 方法2.1：可以写UDF2~UDF20，就是把输入字段变成多个
        spark.udf().register(
                "getLength2",
                new UDF2<Long, String, Long>() {

                    @Override
                    public Long call(Long aLong, String s) throws Exception {
                        return aLong + s.length();
                    }
                },
                DataTypes.LongType);

        inputData = inputData.withColumn(
                "name_length_method2",
                functions.callUDF(
                        "getLength2",
                        inputData.col("id"),
                        inputData.col("name"))
        );

        inputData.show(20, false);

        spark.stop();
    }
}

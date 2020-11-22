package com.tao.workshop.day04;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * 　　　　　　　 ┏┓       ┏┓+ +
 * 　　　　　　　┏┛┻━━━━━━━┛┻┓ + +
 * 　　　　　　　┃　　　　　　 ┃
 * 　　　　　　　┃　　　━　　　┃ ++ + + +
 * 　　　　　　 █████━█████  ┃+
 * 　　　　　　　┃　　　　　　 ┃ +
 * 　　　　　　　┃　　　┻　　　┃
 * 　　　　　　　┃　　　　　　 ┃ + +
 * 　　　　　　　┗━━┓　　　 ┏━┛
 * ┃　　  ┃
 * 　　　　　　　　　┃　　  ┃ + + + +
 * 　　　　　　　　　┃　　　┃　Code is far away from     bug with the animal protecting
 * 　　　　　　　　　┃　　　┃ + 　　　　         神兽保佑,代码无bug
 * 　　　　　　　　　┃　　　┃
 * 　　　　　　　　　┃　　　┃　　+
 * 　　　　　　　　　┃　 　 ┗━━━┓ + +
 * 　　　　　　　　　┃ 　　　　　┣┓
 * 　　　　　　　　　┃ 　　　　　┏┛
 * 　　　　　　　　　┗┓┓┏━━━┳┓┏┛ + + + +
 * 　　　　　　　　　 ┃┫┫　 ┃┫┫
 * 　　　　　　　　　 ┗┻┛　 ┗┻┛+ + + +
 */
public class DataToActivityBeanFunction extends RichMapFunction<String,ActivityBean> {
    private Connection connection = null;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        connection = DriverManager.getConnection("jdbc:mysql://bigdata1:3306/flink-study?characterEncoding=UTF-8", "root", "hadoop");
    }

    @Override
    public ActivityBean map(String line) throws Exception {
        String[] fields = line.split(",");
        String uid = fields[0];
        String aid = fields[1];
        PreparedStatement preparedStatement = connection.prepareStatement("select name from t_activities where id=? ");
        preparedStatement.setString(1,aid);
        ResultSet resultSet = preparedStatement.executeQuery();
        String name = null;
        while(resultSet.next()){
            name = resultSet.getString(1);
            break;
        }
        preparedStatement.close();
        //根据aid作为查询条件查询出name
        String time = fields[2];
        int eventType = Integer.parseInt(fields[3]);
        String province = fields[4];
        return ActivityBean.of(uid,aid,name,time,eventType,province);
    }

    @Override
    public void close() throws Exception {
        super.close();
        connection.close();
    }
}

package com.sitech.utils;

import org.apache.flink.api.java.utils.ParameterTool;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.io.Serializable;

/**
 * Author: EtherealQ (XQ)
 * Date: 2023/3/16
 * Description:启动配置参数POJO
 *使用
 * --path "json文件路径"
 * --maxOutOfOrderness "水印值(单位秒)"
 * --stateTtlTime "数据去除时间阈值(单位秒)"
 * --windowSize "window时间大小(单位秒)"
 * --writeParallelism "write iceberg并行度"
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
public class Parameter implements Serializable {
    public String path;
    public int maxOutOfOrderness;
    public int stateTtlTime;
    public int windowSize;
    public int writeParallelism;
    public String fieldsPath;

    public Parameter(ParameterTool tool) {

        if (tool.has("fieldsPath")){
            this.fieldsPath = tool.get("fieldsPath");
        }else {
            this.fieldsPath = null;
        }

        if (tool.has("path")){
            this.path = tool.get("path");
        }else {
            throw new NullPointerException("path is" + this.path + ", not found.");
        }
        if (tool.has("maxOutOfOrderness")){
            this.maxOutOfOrderness = tool.getInt("maxOutOfOrderness");
        }else {
            this.maxOutOfOrderness = 1;
        }
        if (tool.has("stateTtlTime")){
            this.stateTtlTime = tool.getInt("stateTtlTime");
        }else {
            this.stateTtlTime = 30;
        }
        if (tool.has("windowSize")){
            this.windowSize = tool.getInt("windowSize");
        }else {
            this.windowSize = 5;
        }

        if (tool.has("writeParallelism")){
            this.writeParallelism = tool.getInt("writeParallelism");
        }else {
            this.writeParallelism = 1;
        }
        if (tool.has("interval")){
            this.writeParallelism = tool.getInt("interval");
        }else {
            this.writeParallelism = 60000;
        }

    }
}
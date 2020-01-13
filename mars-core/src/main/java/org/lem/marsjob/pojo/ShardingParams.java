package org.lem.marsjob.pojo;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class ShardingParams implements Serializable {
    //当前节点序号0开始
    private int index;
    //总节点数
    private int total;

    public ShardingParams(int index, int total) {
        this.index = index;
        this.total = total;
    }

    public ShardingParams() {
    }

    public int getIndex() {
        return index;
    }

    public void setIndex(int index) {
        this.index = index;
    }

    public int getTotal() {
        return total;
    }

    public void setTotal(int total) {
        this.total = total;
    }

    @Override
    public String toString() {
        return "ShardingParams{" +
                "index=" + index +
                ", total=" + total +
                '}';
    }

    public static void main(String[] args) {
        JobParam jobParam=new JobParam(JobParam.class,"","","");
        Map<String,Object> map=new HashMap<>();
        jobParam.setJobData(map);
    }
}

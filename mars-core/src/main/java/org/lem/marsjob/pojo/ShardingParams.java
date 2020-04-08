package org.lem.marsjob.pojo;

import com.google.common.base.Objects;

import java.io.Serializable;

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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ShardingParams params = (ShardingParams) o;
        return index == params.index &&
                total == params.total;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(index, total);
    }
}

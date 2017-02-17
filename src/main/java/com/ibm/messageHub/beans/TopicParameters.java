package com.ibm.messageHub.beans;

public class TopicParameters {

    private String name;

    private int partitions;

    private TopicConfig configs;

    public TopicParameters() {
        super();
    }

    public TopicParameters(String name, int partitions, TopicConfig configs) {
        super();
        this.name = name;
        this.partitions = partitions;
        this.configs = configs;
    }

    public String getName() {

        return name;
    }

    public void setName(String name) {

        this.name = name;
    }

    public int getPartitions() {

        return partitions;
    }

    public void setPartitions(int partitions) {

        this.partitions = partitions;
    }

    public TopicConfig getConfigs() {

        return configs;
    }

    public void setConfigs(TopicConfig configs) {

        this.configs = configs;
    }
}

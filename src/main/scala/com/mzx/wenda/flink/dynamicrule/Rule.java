package com.mzx.wenda.flink.dynamicrule;

import java.util.List;

public class Rule {
    private int ruleId;
    private String ruleState;
    private List<String> groupingKeyNames;
    private Long duration;
    private int maxTimes;

    public Rule(int ruleId, String ruleState, List<String> groupingKeyNames, Long duration, int maxTimes) {
        this.ruleId = ruleId;
        this.ruleState = ruleState;
        this.groupingKeyNames = groupingKeyNames;
        this.duration = duration;
        this.maxTimes = maxTimes;
    }

    public int getRuleId() {
        return ruleId;
    }

    public void setRuleId(int ruleId) {
        this.ruleId = ruleId;
    }

    public String getRuleState() {
        return ruleState;
    }

    public void setRuleState(String ruleState) {
        this.ruleState = ruleState;
    }

    public List<String> getGroupingKeyNames() {
        return groupingKeyNames;
    }

    public void setGroupingKeyNames(List<String> groupingKeyNames) {
        this.groupingKeyNames = groupingKeyNames;
    }

    public Long getDuration() {
        return duration;
    }

    public void setDuration(Long duration) {
        this.duration = duration;
    }

    public int getMaxTimes() {
        return maxTimes;
    }

    public void setMaxTimes(int maxTimes) {
        this.maxTimes = maxTimes;
    }
}

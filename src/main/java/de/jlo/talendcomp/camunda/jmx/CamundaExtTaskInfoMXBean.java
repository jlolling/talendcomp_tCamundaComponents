package de.jlo.talendcomp.camunda.jmx;

public interface CamundaExtTaskInfoMXBean {
	
	public int getFetchAndLocksCount();
	
	public int getFetchAndLockRetriesMin();

	public int getFetchAndLockRetriesMax();

	public double getFetchAndLockRetriesAvg();

	public double getFetchAndLockRetriesAvgPeriod();

	public int getFetchAndLockRetriesTotal();

	public int getFetchedTasksTotal();

	public int getFetchedTasksMin();

	public int getFetchedTasksMax();

	public double getFetchedTasksAvg();

	public double getFetchedTasksAvgPeriod();

	public long getFetchAndLockDurationMin();

	public long getFetchAndLockDurationMax();

	public double getFetchAndLockDurationAvg();

	public double getFetchAndLockDurationAvgPeriod();

	public long getFetchAndLockDurationTotal();

	public String[] getLastFetchErrorResponses();

	public int getCompletsCount();
	
	public int getCompletsCountPeriod();

	public int getCompleteRetriesMin();

	public int getCompleteRetriesMax();

	public double getCompleteRetriesAvg();

	public double getCompleteRetriesAvgPeriod();

	public int getCompleteRetriesTotal();

	public long getCompleteDurationMin();

	public long getCompleteDurationMax();

	public double getCompleteDurationAvg();
	
	public double getCompleteDurationAvgPeriod();

	public long getCompleteDurationTotal();
	
	public int getTaskProcessedCount();
	
	public int getTaskProcessedCountPeriod();

	public long getTaskProcessingDurationMin();

	public long getTaskProcessingDurationMax();

	public double getTaskProcessingDurationAvg();

	public double getTaskProcessingDurationAvgPeriod();

	public String[] getLastCompleteErrorResponses();

	public int getBpmnErrorCount();
	
	public int getBpmnErrorCountPeriod();

	public int getFailureCount();
	
	public int getFailureCountPeriod();

	public String getTopic();
	
	public String getCamundaExtTaskURL();
	
	public int getFetchSize();
	
	public void setFetchSize(int size);
	
	public long getLockDuration();
	
	public void setLockDuration(long duration);
	
	public void stopFetching();
	
	public long getMeasurementRefreshTime();
	
	public long getMeasurementLastTime();

}

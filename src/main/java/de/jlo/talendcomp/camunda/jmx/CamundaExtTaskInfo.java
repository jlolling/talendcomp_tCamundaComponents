package de.jlo.talendcomp.camunda.jmx;

import java.util.Date;
import java.util.Timer;
import java.util.TimerTask;

import de.jlo.talendcomp.camunda.externaltask.FetchAndLock;

public class CamundaExtTaskInfo implements CamundaExtTaskInfoMXBean {
	
	private long refreshTime = 10000l;
	private long lastMeasured = 0l;
	private int countFetchAndLocks = 0;
	private int countFetchAndLocksLast = 0;
	private int countFetchAndLocksDiff = 0;
	private int minFetchAndLockRetries = 0;
	private int maxFetchAndLockRetries = 0;
	private int totalFetchAndLockRetries = 0;
	private int totalFetchAndLockRetriesLast = 0;
	private int totalFetchAndLockRetriesDiff = 0;
	private int minFetchedTasks = 0;
	private int maxFetchedTasks = 0;
	private int totalFetchedTasks = 0;
	private int totalFetchedTasksLast = 0;
	private int totalFetchedTasksDiff = 0;
	private long minFetchAndLockDuration = 0l;
	private long maxFetchAndLockDuration = 0l;
	private long totalFetchAndLockDuration = 0l;
	private long totalFetchAndLockDurationLast = 0l;
	private long totalFetchAndLockDurationDiff = 0l;
	private int totalComplets = 0;
	private int totalCompletsLast = 0;
	private int totalCompletsDiff = 0;
	private int minCompleteRetries = 0;
	private int maxCompleteRetries = 0;
	private int totalCompleteRetries = 0;
	private int totalCompleteRetriesLast = 0;
	private int totalCompleteRetriesDiff = 0;
	private long minCompleteDuration = 0l;
	private long maxCompleteDuration = 0l;
	private long totalCompleteDuration = 0l;
	private long totalCompleteDurationLast = 0l;
	private long totalCompleteDurationDiff = 0l;
	private int countBpmnErrors = 0;
	private int countBpmnErrorsLast = 0;
	private int countBpmnErrorsDiff = 0;
	private int countFailures = 0;
	private int countFailuresLast = 0;
	private int countFailuresDiff = 0;
	private long minTaskProcessingDuration = 0l;
	private long maxTaskProcessingDuration = 0l;
	private long totalTaskProcessingDuration = 0l;
	private long totalTaskProcessingDurationLast = 0l;
	private long totalTaskProcessingDurationDiff = 0l;
	private int countTotalFinished = 0;
	private int countTotalFinishedLast = 0;
	private int countTotalFinishedDiff = 0;
	private FetchAndLock fetchAndLock = null;
	private Timer timer = null;
	
	public CamundaExtTaskInfo() {
		timer = new Timer(true);
		timer.schedule(new TimerTask() {
			
			@Override
			public void run() {
				snapshot();
			}
		}, refreshTime, refreshTime);
	}
	
	private void snapshot() {
		countFetchAndLocksDiff = countFetchAndLocks - countFetchAndLocksLast;
		countFetchAndLocksLast = countFetchAndLocks;
		totalFetchAndLockDurationDiff = totalFetchAndLockDuration - totalFetchAndLockDurationLast;
		totalFetchAndLockDurationLast = totalFetchAndLockDuration;
		totalCompletsDiff = totalComplets - totalCompletsLast;
		totalCompletsLast = totalComplets;
		totalCompleteRetriesDiff = totalCompleteRetries - totalCompleteRetriesLast;
		totalCompleteRetriesLast = totalCompleteRetries;
		totalCompleteDurationDiff = totalCompleteDuration - totalCompleteDurationLast;
		totalCompleteDurationLast = totalCompleteDuration;
		countTotalFinishedDiff = countTotalFinished - countTotalFinishedLast;
		countTotalFinishedLast = countTotalFinished;
		totalFetchAndLockRetriesDiff = totalFetchAndLockRetries - totalFetchAndLockRetriesLast;
		totalFetchAndLockRetriesLast = totalFetchAndLockRetries;
		totalTaskProcessingDurationDiff = totalTaskProcessingDuration - totalTaskProcessingDurationLast;
		totalTaskProcessingDurationLast = totalTaskProcessingDuration;
		totalFetchedTasksDiff = totalFetchedTasks - totalFetchedTasksLast;
		totalFetchedTasksLast = totalFetchedTasks;
		countBpmnErrorsDiff = countBpmnErrors - countBpmnErrorsLast;
		countBpmnErrorsLast = countBpmnErrors;
		countFailuresDiff = countFailures - countFailuresLast;
		countFailuresLast = countFailures;
		lastMeasured = System.currentTimeMillis();
	}
	
	public void stop() {
		if (timer != null) {
			timer.cancel();
		}
	}
	
	public void setFetchAndLock(FetchAndLock fetchAndLock) {
		this.fetchAndLock = fetchAndLock;
	}
	
	public void addFetchAndLock(long duration, int countRetries, String errorMessage) {
		this.totalFetchAndLockDuration += duration;
		this.totalFetchAndLockRetries += countRetries;
		this.countFetchAndLocks++;
		if (maxFetchAndLockDuration < duration) {
			maxFetchAndLockDuration = duration;
		}
		if (minFetchAndLockDuration == 0l || minFetchAndLockDuration > duration) {
			minFetchAndLockDuration = duration;
		}
	}
	
	public void addComplete(long duration, int countRetries, String errorMessage) {
		this.totalCompleteDuration += duration;
		this.totalCompleteRetries += countRetries;
		this.totalComplets++;
		if (maxCompleteDuration < duration) {
			maxCompleteDuration = duration;
		}
		if (minCompleteDuration == 0l || minCompleteDuration > duration) {
			minCompleteDuration = duration;
		}
	}

	public void addFailure() {
		this.countFailures++;
	}

	public void addBpmnError() {
		this.countBpmnErrors++;
	}

	public void addTaskFinished(long duration) {
		totalTaskProcessingDuration += duration;
		this.countTotalFinished++;
		if (maxTaskProcessingDuration < duration) {
			maxTaskProcessingDuration = duration;
		}
		if (minTaskProcessingDuration == 0l || minTaskProcessingDuration > duration) {
			minTaskProcessingDuration = duration;
		}
	}

	public void addTaskFetched(int number) {
		totalFetchedTasks += number;
		if (maxFetchedTasks < number) {
			maxFetchedTasks = number;
		}
		if (minFetchedTasks == 0 || minFetchedTasks > number) {
			minFetchedTasks = number;
		}
	}
	
	@Override
	public int getFetchAndLocksCount() {
		return countFetchAndLocks;
	}

	@Override
	public int getFetchAndLockRetriesMin() {
		return minFetchAndLockRetries;
	}

	@Override
	public int getFetchAndLockRetriesMax() {
		return maxFetchAndLockRetries;
	}

	@Override
	public double getFetchAndLockRetriesAvg() {
		if (countFetchAndLocks > 0) {
			return ((double) totalFetchAndLockRetries) / ((double) countFetchAndLocks);
		} else {
			return 0d;
		}
	}

	@Override
	public double getFetchAndLockRetriesAvgPeriod() {
		if (countFetchAndLocksDiff > 0) {
			return ((double) totalFetchAndLockRetriesDiff) / ((double) countFetchAndLocksDiff);
		} else {
			return 0d;
		}
	}

	@Override
	public int getFetchAndLockRetriesTotal() {
		return totalFetchAndLockRetries;
	}

	@Override
	public long getFetchAndLockDurationMin() {
		return minFetchAndLockDuration;
	}

	@Override
	public long getFetchAndLockDurationMax() {
		return maxFetchAndLockDuration;
	}
	
	@Override
	public long getFetchAndLockDurationTotal() {
		return totalFetchAndLockDuration;
	}

	public void setSumFetchAndLockDuration(long sumFetchAndLockDuration) {
		this.totalFetchAndLockDuration = sumFetchAndLockDuration;
	}

	@Override
	public double getFetchAndLockDurationAvg() {
		if (countFetchAndLocks > 0l) {
			return ((double) totalFetchAndLockDuration) / ((double) countFetchAndLocks);
		} else {
			return 0l;
		}
	}

	@Override
	public double getFetchAndLockDurationAvgPeriod() {
		if (countFetchAndLocksDiff > 0l) {
			return ((double) totalFetchAndLockDurationDiff) / ((double) countFetchAndLocksDiff);
		} else {
			return 0l;
		}
	}

	@Override
	public int getCompletsCount() {
		return totalComplets;
	}

	@Override
	public int getCompletsCountPeriod() {
		return totalCompletsDiff;
	}

	@Override
	public int getCompleteRetriesMin() {
		return minCompleteRetries;
	}

	@Override
	public int getCompleteRetriesMax() {
		return maxCompleteRetries;
	}

	@Override
	public double getCompleteRetriesAvg() {
		if (totalComplets > 0) {
			return ((double) totalCompleteRetries) / ((double) totalComplets);
		} else {
			return 0;
		}
	}

	@Override
	public double getCompleteRetriesAvgPeriod() {
		if (totalCompletsDiff > 0) {
			return ((double) totalCompleteRetriesDiff) / ((double) totalCompletsDiff);
		} else {
			return 0;
		}
	}

	@Override
	public int getCompleteRetriesTotal() {
		return totalCompleteRetries;
	}

	@Override
	public long getCompleteDurationMin() {
		return minCompleteDuration;
	}

	@Override
	public long getCompleteDurationMax() {
		return maxCompleteDuration;
	}

	@Override
	public double getCompleteDurationAvg() {
		if (totalComplets > 0) {
			return ((double) totalCompleteDuration) / ((double) totalComplets);
		} else {
			return 0;
		}
	}
	
	@Override
	public double getCompleteDurationAvgPeriod() {
		if (totalCompletsDiff > 0) {
			return ((double) totalCompleteDurationDiff) / ((double) totalCompletsDiff);
		} else {
			return 0;
		}
	}

	@Override
	public long getCompleteDurationTotal() {
		return totalCompleteDuration;
	}

	@Override
	public int getBpmnErrorCount() {
		return countBpmnErrors;
	}

	@Override
	public int getBpmnErrorCountPeriod() {
		return countBpmnErrorsDiff;
	}

	@Override
	public int getFailureCount() {
		return countFailures;
	}
	
	@Override
	public int getFailureCountPeriod() {
		return countFailuresDiff;
	}
	
	@Override
	public long getTaskProcessingDurationMin() {
		return minTaskProcessingDuration;
	}

	@Override
	public long getTaskProcessingDurationMax() {
		return maxTaskProcessingDuration;
	}
	
	@Override
	public double getTaskProcessingDurationAvg() {
		if (countTotalFinished > 0) {
			return ((double) totalTaskProcessingDuration) / ((double) countTotalFinished);
		}
		return 0l; 
	}

	@Override
	public double getTaskProcessingDurationAvgPeriod() {
		if (countTotalFinishedDiff > 0) {
			return ((double) totalTaskProcessingDurationDiff) / ((double) countTotalFinishedDiff);
		}
		return 0l; 
	}

	@Override
	public String getTopic() {
		if (fetchAndLock != null) {
			return fetchAndLock.getTopicName();
		} else {
			return null;
		}
	}

	@Override
	public String getCamundaExtTaskURL() {
		return fetchAndLock.getExternalTaskEndpointURL();
	}

	@Override
	public int getTaskProcessedCount() {
		return countTotalFinished;
	}

	@Override
	public int getTaskProcessedCountPeriod() {
		return countTotalFinishedDiff;
	}

	@Override
	public int getFetchSize() {
		if (fetchAndLock != null) {
			return fetchAndLock.getNumberTaskToFetch();
		} else {
			return 0;
		}
	}

	@Override
	public void setFetchSize(int size) {
		if (fetchAndLock != null) {
			fetchAndLock.setNumberTaskToFetch(size);
		}
	}

	@Override
	public long getLockDuration() {
		if (fetchAndLock != null) {
			return fetchAndLock.getLockDuration();
		} else {
			return 0l;
		}
	}
	
	@Override
	public void setLockDuration(long duration) {
		if (fetchAndLock != null) {
			fetchAndLock.setLockDuration(duration);
		}
	}

	@Override
	public void stopFetching() {
		if (fetchAndLock != null) {
			fetchAndLock.setStopTime(new Date());
		}
	}

	@Override
	public int getFetchedTasksTotal() {
		return totalFetchedTasks;
	}

	@Override
	public int getFetchedTasksMin() {
		return minFetchedTasks;
	}

	@Override
	public int getFetchedTasksMax() {
		return maxFetchedTasks;
	}

	@Override
	public double getFetchedTasksAvg() {
		if (countFetchAndLocks > 0) {
			return ((double) totalFetchedTasks) / ((double) countFetchAndLocks);
		} else {
			return 0d;
		}
	}

	@Override
	public double getFetchedTasksAvgPeriod() {
		if (countFetchAndLocksDiff > 0) {
			return ((double) totalFetchedTasksDiff) / ((double) countFetchAndLocksDiff);
		} else {
			return 0d;
		}
	}

	@Override
	public long getMeasurementRefreshTime() {
		return refreshTime;
	}

	public void setMeasurementRefreshTime(long refreshTime) {
		this.refreshTime = refreshTime;
	}
	
	@Override
	public long getMeasurementLastTime() {
		return lastMeasured;
	}

}

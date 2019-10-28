package de.jlo.talendcomp.camunda.jmx;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Timer;
import java.util.TimerTask;

import de.jlo.talendcomp.camunda.externaltask.FetchAndLock;

public class CamundaExtTaskInfo implements CamundaExtTaskInfoMXBean {
	
	private long refreshTime = 10000l;
	private long lastMeasured = 0l;
	private int totalFetchAndLocks = 0;
	private int totalFetchAndLocksLast = 0;
	private int totalFetchAndLocksDiff = 0;
	private int minFetchAndLockRetries = 0;
	private int maxFetchAndLockRetries = 0;
	private int totalFetchAndLocksWithRetries = 0;
	private int totalFetchAndLocksWithRetriesDiff = 0;
	private int totalFetchAndLocksWithRetriesLast = 0;
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
	private int totalCompletsWithRetries = 0;
	private int totalCompletsWithRetriesDiff = 0;
	private int totalCompletsWithRetriesLast = 0;
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
	private long lastTaskFetchedTime = 0l;
	private String lastTaskFetchedTimeAsString = null;
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
	private long workerStartTime = 0;
	private String workerStartTimeString = null;
	private FetchAndLock fetchAndLock = null;
	private Timer timer = null;
	private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
	
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
		totalFetchAndLocksDiff = totalFetchAndLocks - totalFetchAndLocksLast;
		totalFetchAndLocksLast = totalFetchAndLocks;
		totalFetchAndLocksWithRetriesDiff = totalFetchAndLocksWithRetries - totalFetchAndLocksWithRetriesLast;
		totalFetchAndLocksWithRetriesLast = totalFetchAndLocksWithRetries;
		totalFetchAndLockDurationDiff = totalFetchAndLockDuration - totalFetchAndLockDurationLast;
		totalFetchAndLockDurationLast = totalFetchAndLockDuration;
		totalCompletsDiff = totalComplets - totalCompletsLast;
		totalCompletsLast = totalComplets;
		totalCompletsWithRetriesDiff = totalCompletsWithRetries - totalCompletsWithRetriesLast;
		totalCompletsWithRetriesLast = totalCompletsWithRetries;
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
	
	public void addFetchAndLock(long duration, int countRetries) {
		this.totalFetchAndLocks++;
		if (countRetries > 0) {
			this.totalFetchAndLocksWithRetries++;
		}
		this.totalFetchAndLockDuration += duration;
		if (maxFetchAndLockDuration < duration) {
			maxFetchAndLockDuration = duration;
		}
		if (minFetchAndLockDuration == 0l || minFetchAndLockDuration > duration) {
			minFetchAndLockDuration = duration;
		}
		this.totalFetchAndLockRetries += countRetries;
		if (maxFetchAndLockRetries < countRetries) {
			maxFetchAndLockRetries = countRetries;
		}
		if (minFetchAndLockRetries == 0l || minFetchAndLockRetries > countRetries) {
			minFetchAndLockRetries = countRetries;
		}
	}
	
	public void addComplete(long duration, int countRetries) {
		this.totalComplets++;
		if (countRetries > 0) {
			this.totalCompletsWithRetries++;
		}
		this.totalCompleteDuration += duration;
		if (maxCompleteRetries < countRetries) {
			maxCompleteRetries = countRetries;
		}
		if (minCompleteRetries == 0l || minCompleteRetries > countRetries) {
			minCompleteRetries = countRetries;
		}
		this.totalCompleteRetries += countRetries;
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
		this.countTotalFinished++;
		totalTaskProcessingDuration += duration;
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
		if (number > 0) {
			lastTaskFetchedTime = System.currentTimeMillis();
			lastTaskFetchedTimeAsString = sdf.format(new Date(lastTaskFetchedTime));
		}
	}
	
	@Override
	public int getFetchAndLocksCount() {
		return totalFetchAndLocks;
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
		if (totalFetchAndLocksWithRetries > 0) {
			return ((double) totalFetchAndLockRetries) / ((double) totalFetchAndLocksWithRetries);
		} else {
			return 0d;
		}
	}

	@Override
	public double getFetchAndLockRetriesAvgPeriod() {
		if (totalFetchAndLocksWithRetriesDiff > 0) {
			return ((double) totalFetchAndLockRetriesDiff) / ((double) totalFetchAndLocksWithRetriesDiff);
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
		if (totalFetchAndLocks > 0l) {
			return ((double) totalFetchAndLockDuration) / ((double) totalFetchAndLocks);
		} else {
			return 0l;
		}
	}

	@Override
	public double getFetchAndLockDurationAvgPeriod() {
		if (totalFetchAndLocksDiff > 0l) {
			return ((double) totalFetchAndLockDurationDiff) / ((double) totalFetchAndLocksDiff);
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
		if (totalCompletsWithRetries > 0) {
			return ((double) totalCompleteRetries) / ((double) totalCompletsWithRetries);
		} else {
			return 0;
		}
	}

	@Override
	public double getCompleteRetriesAvgPeriod() {
		if (totalCompletsWithRetriesDiff > 0) {
			return ((double) totalCompleteRetriesDiff) / ((double) totalCompletsWithRetriesDiff);
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
		if (totalFetchAndLocks > 0) {
			return ((double) totalFetchedTasks) / ((double) totalFetchAndLocks);
		} else {
			return 0d;
		}
	}

	@Override
	public double getFetchedTasksAvgPeriod() {
		if (totalFetchAndLocksDiff > 0) {
			return ((double) totalFetchedTasksDiff) / ((double) totalFetchAndLocksDiff);
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

	@Override
	public long getWorkerStartTime() {
		return workerStartTime;
	}

	public void setWorkerStartTime(long workerStartTime) {
		this.workerStartTime = workerStartTime;
		this.workerStartTimeString = sdf.format(new Date(workerStartTime));
	}

	@Override
	public String getWorkerStartTimeAsString() {
		return workerStartTimeString;
	}

	@Override
	public long getLastTaskFetchedTime() {
		return lastTaskFetchedTime;
	}

	@Override
	public String getLastTaskFetchedTimeAsString() {
		return lastTaskFetchedTimeAsString;
	}

	@Override
	public boolean getUseLongPolling() {
		return fetchAndLock.isUseLongPolling();
	}

	@Override
	public long getAsyncResponseTimeout() {
		return fetchAndLock.getAsyncResponseTimeout();
	}

}

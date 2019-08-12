package de.jlo.talendcomp.camunda.externaltask;

import java.text.SimpleDateFormat;
import java.util.Date;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.jayway.jsonpath.internal.Utils;

import de.jlo.talendcomp.camunda.CamundaClient;
import de.jlo.talendcomp.camunda.HttpClient;
import de.jlo.talendcomp.camunda.Util;
import de.jlo.talendcomp.camunda.jmx.CamundaExtTaskInfo;

public class Response extends CamundaClient {
	
	private String taskId = null;
	private String taskIdFromInput = null;
	private String processInstanceId = null;
	private String processInstanceIdFromInput = null;
	private Date taskExpirationTimeFromInput = null;
	private ObjectNode currentVariablesNode = null;
	private FetchAndLock fetchAndLock = null;
	private boolean checkLockExpiration = true;
	private boolean suppressExpiredTasks = false;
	private boolean currentTaskExpired = false;
	private CamundaExtTaskInfo mbeanCamundaExtTaskInfo = null;
	private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
	
	public Response(FetchAndLock fetchAndLock) throws Exception {
		if (fetchAndLock == null) {
			throw new IllegalArgumentException("The fetchAndLock component cannot be null");
		}
		this.fetchAndLock = fetchAndLock;
		this.setCamundaServiceURL(fetchAndLock.getCamundaServiceURL());
		this.setAlternateEndpoint(fetchAndLock.getAlternateEndpoint());
		this.setCamundaEngine(fetchAndLock.getCamundaEngine());
		this.setHttpClient(fetchAndLock.getHttpClient());
		this.mbeanCamundaExtTaskInfo = fetchAndLock.getCamundaExtTaskInfo();
	}
	
	public void addVariable(String varName, Object value, String pattern, String dataObjectTypName, String type) {
		if (currentVariablesNode == null) {
			currentVariablesNode = objectMapper.createObjectNode();
		}
		addVariableNode(currentVariablesNode, varName, value, pattern, dataObjectTypName, type);
	}
	
	private void setupTaskId() {
		if (Util.isEmpty(taskIdFromInput) == false) {
			taskId = taskIdFromInput;
		} else {
			if (fetchAndLock.isReturnAllTasksCurrentlyFetched()) {
				throw new IllegalStateException("Fetch and Lock is configured to return all fetched tasks at once as array. Therefore it is not possible to take the current task-id from the fetchAndLock component. Please provide the task-id via the input flow!");
			}
			taskId = fetchAndLock.getCurrentTaskId();
		}
		if (taskId == null) {
			throw new IllegalStateException("Task-Id not provided by the fetchAndLock component");
		}
	}
	
	private Date getTaskExpirationTime() throws Exception {
		if (fetchAndLock.isReturnAllTasksCurrentlyFetched()) {
			if (taskExpirationTimeFromInput == null) {
				throw new IllegalStateException("Fetch and Lock is configured to return all fetched tasks at once as array. Therefore it is not possible to take the current task expiration time from the fetchAnLock component. Please provide the task expiration time via the separate input.");
			}
			return taskExpirationTimeFromInput;
		} else {
			return fetchAndLock.getCurrentTaskLockExpirationTime();
		}
	}
	
	private void checkTaskExpiration() throws Exception {
		if (checkLockExpiration) {
			Date taskLockExpirationTime = getTaskExpirationTime();
			Date now = new Date();
			if (taskLockExpirationTime != null && now.before(taskLockExpirationTime) == false) {
				currentTaskExpired = true;
				String messageTimePart = "Expected response time: " + sdf.format(taskLockExpirationTime) + ", response attempt at: " + sdf.format(now);
				if (suppressExpiredTasks) {
					System.err.println("Task id: " + taskId + " has been expired and will be ignored. " + messageTimePart);
				} else {
					throw new Exception("Task id: " + taskId + " has been expired, Stop fetching. " + messageTimePart);
				}
			}
		}
	}
	
	private void notifiyFinishedTask() {
		if (mbeanCamundaExtTaskInfo != null) {
			long taskProcessingStopTime = System.currentTimeMillis();
			long taskProcessingDuration = taskProcessingStopTime - fetchAndLock.getCurrentTaskStartTime();
			mbeanCamundaExtTaskInfo.addTaskFinished(taskProcessingDuration);
		}
	}
	
	public void complete() throws Exception {
		notifiyFinishedTask();
		currentTaskExpired = false;
		String workerId = fetchAndLock.getWorkerId();
		if (workerId == null) {
			throw new IllegalStateException("workerId not provided by the fetchAndLock component");
		}
		setupTaskId();
		checkTaskExpiration();
		ObjectNode requestPayload = objectMapper.createObjectNode();
		requestPayload.put("workerId", workerId);
		requestPayload.set("variables", currentVariablesNode);
		currentVariablesNode = null; // set node to null to force creating a new one for the next complete call
		HttpClient client = getHttpClient();
		long startTime = System.currentTimeMillis();
		client.post(getExternalTaskEndpointURL() + "/" + taskId + "/complete", requestPayload, false);
		int countRetries = client.getCurrentAttempt();
		long duration = System.currentTimeMillis() - startTime;
		if (mbeanCamundaExtTaskInfo != null) {
			mbeanCamundaExtTaskInfo.addComplete(duration, countRetries);
		}
	}
	
	public void bpmnError(String errorCode) throws Exception {
		notifiyFinishedTask();
		String workerId = fetchAndLock.getWorkerId();
		if (workerId == null) {
			throw new IllegalStateException("workerId not provided by the fetchAndLock component");
		}
		setupTaskId();
		ObjectNode requestPayload = objectMapper.createObjectNode();
		requestPayload.put("workerId", workerId);
		requestPayload.put("errorCode", errorCode);
		HttpClient client = getHttpClient();
		client.post(getExternalTaskEndpointURL() + "/" + taskId + "/bpmnError", requestPayload, false);
		if (mbeanCamundaExtTaskInfo != null) {
			mbeanCamundaExtTaskInfo.addBpmnError();
		}
	}
	
	public void updateProcessVariables() throws Exception {
		setupProcessInstanceId();
		HttpClient client = getHttpClient();
		ObjectNode requestPayload = objectMapper.createObjectNode();
		requestPayload.set("modifications", currentVariablesNode);
		client.post(getProcessVariablesEndpointURL(processInstanceId), requestPayload, false);
//		if (client.getStatusCode() != 204) {
//			String message = "Update variables POST-payload: \n" + requestPayload.toString() + "\n failed: status-code: " + client.getStatusCode() + " message: " + client.getStatusMessage();
//			throw new Exception(message);
//		}
	}

	public void failure(String errorMessage, String errorDetails, Integer retries, Number retryTimeout) throws Exception {
		notifiyFinishedTask();
		String workerId = fetchAndLock.getWorkerId();
		if (workerId == null) {
			throw new IllegalStateException("workerId not provided by the fetchAndLock component");
		}
		setupTaskId();
		ObjectNode requestPayload = objectMapper.createObjectNode();
		requestPayload.put("workerId", workerId);
		requestPayload.put("errorMessage", errorMessage);
		if (Utils.isEmpty(errorDetails) == false) {
			requestPayload.put("errorDetails", errorDetails);
		}
		if (retries != null && retries > 0) {
			requestPayload.put("retries", retries);
		} else {
			requestPayload.put("retries", 0);
		}
		if (retryTimeout != null && retryTimeout.intValue() > 0) {
			requestPayload.put("retryTimeout", retryTimeout.intValue());
		}
		HttpClient client = getHttpClient();
		client.post(getExternalTaskEndpointURL() + "/" + taskId + "/failure", requestPayload, false);
		if (mbeanCamundaExtTaskInfo != null) {
			mbeanCamundaExtTaskInfo.addFailure();
		}
	}
	
	public void unlock() throws Exception {
		notifiyFinishedTask();
		setupTaskId();
		HttpClient client = getHttpClient();
		client.post(getExternalTaskEndpointURL() + "/" + taskId + "/unlock", null, false);
	}

	public String getTaskId() {
		return taskId;
	}

	public boolean isCheckLockExpiration() {
		return checkLockExpiration;
	}

	public void setCheckLockExpiration(Boolean checkLockExpiration) {
		if (checkLockExpiration != null) {
			this.checkLockExpiration = checkLockExpiration.booleanValue();
		}
	}

	public boolean isSuppressExpiredTasks() {
		return suppressExpiredTasks;
	}

	public void setSuppressExpiredTasks(Boolean suppressExpiredTasks) {
		if (suppressExpiredTasks != null) {
			this.suppressExpiredTasks = suppressExpiredTasks.booleanValue();
		}
	}

	public boolean isCurrentTaskExpired() {
		return currentTaskExpired;
	}

	public void setCheckLockExpiration(boolean checkLockExpiration) {
		this.checkLockExpiration = checkLockExpiration;
	}

	public String getTaskIdFromInput() {
		return taskIdFromInput;
	}

	public void setTaskIdFromInput(String taskIdFromInput) {
		if (Util.isEmpty(taskIdFromInput)) {
			throw new IllegalArgumentException("Task ID from input data cannot be null or empty!");
		}
		this.taskIdFromInput = taskIdFromInput;
	}

	public void setTaskExpirationTime(Date taskExpirationTime) {
		if (taskExpirationTime == null) {
			throw new IllegalArgumentException("Cannot set null as taskExpirationTime");
		}
		this.taskExpirationTimeFromInput = taskExpirationTime;
	}

	public void setProcessInstanceIdFromInput(String processInstanceId) {
		if (Util.isEmpty(processInstanceId) == false) {
			this.processInstanceIdFromInput = processInstanceId;
		}
	}
	
	private void setupProcessInstanceId() {
		if (Util.isEmpty(processInstanceIdFromInput) == false) {
			processInstanceId = processInstanceIdFromInput;
		} else {
			if (fetchAndLock.isReturnAllTasksCurrentlyFetched()) {
				throw new IllegalStateException("Fetch and Lock is configured to return all fetched tasks at once as array. Therefore it is not possible to take the current process-instance-id from the fetchAndLock component. Please provide the process-instance-id via the input flow!");
			}
			processInstanceId = fetchAndLock.getCurrentTaskProcessInstanceId();
		}
		if (processInstanceId == null) {
			throw new IllegalStateException("Process-Instance-Id not provided by the fetchAndLock component");
		}
	}

}

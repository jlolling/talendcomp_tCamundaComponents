package de.jlo.talendcomp.camunda.externaltask;

import java.util.Date;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.jayway.jsonpath.internal.Utils;

import de.jlo.talendcomp.camunda.CamundaClient;
import de.jlo.talendcomp.camunda.HttpClient;
import de.jlo.talendcomp.camunda.Util;

public class Response extends CamundaClient {

	private String taskId = null;
	private String taskIdFromInput = null;
	private ObjectNode currentVariablesNode = null;
	private FetchAndLock fetchAndLock = null;
	private boolean checkLockExpiration = true;
	private boolean suppressExpiredTasks = false;
	private boolean currentTaskExpired = false;
	private long currentTaskDuration = 0;
	
	public Response(FetchAndLock fetchAndLock) throws Exception {
		if (fetchAndLock == null) {
			throw new IllegalArgumentException("The fetchAndLock component cannot be null");
		}
		this.fetchAndLock = fetchAndLock;
		this.setCamundaServiceURL(fetchAndLock.getCamundaServiceURL());
		this.setAlternateEndpoint(fetchAndLock.getAlternateEndpoint());
		this.setCamundaEngine(fetchAndLock.getCamundaEngine());
		this.setHttpClient(fetchAndLock.getHttpClient());
		this.setDebug(fetchAndLock.isDebug());
	}
	
	public void addVariable(String varName, Object value, String pattern, String dataObjectTypName) {
		if (currentVariablesNode == null) {
			currentVariablesNode = objectMapper.createObjectNode();
		}
		addVariableNode(currentVariablesNode, varName, value, pattern, dataObjectTypName);
	}
	
	public void measureTaskDuration() {
		currentTaskDuration = 0;
		if (fetchAndLock.getCurrentTaskStartTime() > 0) {
			currentTaskDuration = System.currentTimeMillis() - fetchAndLock.getCurrentTaskStartTime();
		}
	}
	
	private void setupTaskId() {
		if (Util.isEmpty(taskIdFromInput) == false) {
			taskId = taskIdFromInput;
		} else {
			taskId = fetchAndLock.getCurrentTaskId();
		}
		if (taskId == null) {
			throw new IllegalStateException("taskId not provided by the fetchAndLock component");
		}
	}
	
	public void complete() throws Exception {
		currentTaskExpired = false;
		String workerId = fetchAndLock.getWorkerId();
		if (workerId == null) {
			throw new IllegalStateException("workerId not provided by the fetchAndLock component");
		}
		setupTaskId();
		if (checkLockExpiration) {
			Date taskLockExpirationTime = fetchAndLock.getCurrentTaskLockExpirationTime();
			if (taskLockExpirationTime != null && new Date().before(taskLockExpirationTime) == false) {
				currentTaskExpired = true;
				if (suppressExpiredTasks) {
					LOG.warn("Task: " + fetchAndLock.getCurrentTask().toString() + " has been expired and will be ignored");
				} else {
					throw new Exception("Lock expiration time exceeded for Task: " + fetchAndLock.getCurrentTask().toString());
				}
			}
		}
		ObjectNode requestPayload = objectMapper.createObjectNode();
		requestPayload.put("workerId", workerId);
		requestPayload.set("variables", currentVariablesNode);
		currentVariablesNode = null; // set node to null to force creating a new one for the next complete call
		HttpClient client = getHttpClient();
		client.post(getExternalTaskEndpointURL() + "/" + taskId + "/complete", requestPayload, false);
		measureTaskDuration();
		if (client.getStatusCode() != 204) {
			String message = "Complete POST-payload: \n" + requestPayload.toString() + "\n failed: status-code: " + client.getStatusCode() + " message: " + client.getStatusMessage();
			LOG.error(message);
			throw new Exception(message);
		}
	}
	
	public void bpmnError(String errorCode) throws Exception {
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
		measureTaskDuration();
		if (client.getStatusCode() != 204) {
			String message = "BpmnError POST-payload: \n" + requestPayload.toString() + "\n failed: status-code: " + client.getStatusCode() + " message: " + client.getStatusMessage();
			LOG.error(message);
			throw new Exception(message);
		}
	}

	public void failure(String errorMessage, String errorDetails, Integer retries, Number retryTimeout) throws Exception {
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
		if (retries != null) {
			requestPayload.put("retries", retries);
		} else {
			requestPayload.put("retries", 0);
		}
		if (retryTimeout != null && retryTimeout.intValue() > 0) {
			requestPayload.put("retryTimeout", retryTimeout.intValue());
		}
		HttpClient client = getHttpClient();
		client.post(getExternalTaskEndpointURL() + "/" + taskId + "/failure", requestPayload, false);
		measureTaskDuration();
		if (client.getStatusCode() != 204) {
			String message = "Failure POST-payload: \n" + requestPayload.toString() + "\n failed: status-code: " + client.getStatusCode() + " message: " + client.getStatusMessage();
			LOG.error(message);
			throw new Exception(message);
		}
	}
	
	public void unlock() throws Exception {
		setupTaskId();
		HttpClient client = getHttpClient();
		client.post(getExternalTaskEndpointURL() + "/" + taskId + "/unlock", null, false);
		measureTaskDuration();
		if (client.getStatusCode() != 204) {
			String message = "Unlock POST failed: status-code: " + client.getStatusCode() + " message: " + client.getStatusMessage();
			LOG.error(message);
			throw new Exception(message);
		}
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

	public long getCurrentTaskDuration() {
		return currentTaskDuration;
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

}

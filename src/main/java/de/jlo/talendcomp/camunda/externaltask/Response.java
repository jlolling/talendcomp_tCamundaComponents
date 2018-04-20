package de.jlo.talendcomp.camunda.externaltask;

import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.Date;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.jayway.jsonpath.internal.Utils;

public class Response extends CamundaClient {

	private String taskId = null;
	private ObjectNode currentVariablesNode = null;
	private FetchAndLock fetchAndLock = null;
	private boolean checkLockExpiration = true;
	private boolean suppressExpiredTasks = false;
	private boolean currentTaskExpired = false;
	
	public Response(FetchAndLock fetchAndLock) {
		if (fetchAndLock == null) {
			throw new IllegalArgumentException("The fetchAndLock component cannot be null");
		}
		this.fetchAndLock = fetchAndLock;
		setCamundaServiceURL(fetchAndLock.getCamundaServiceURL());
		setAlternateEndpoint(fetchAndLock.getAlternateEndpoint());
		setCamundaEngine(fetchAndLock.getCamundaEngine());
		setNeedAuthorization(fetchAndLock.isNeedAuthorization());
		setCamundaUser(fetchAndLock.getCamundaUser());
		setCamundaPassword(fetchAndLock.getCamundaPassword());
		setTimeout(fetchAndLock.getTimeout());
		setMaxRetriesInCaseOfErrors(fetchAndLock.getMaxRetriesInCaseOfErrors());
		setWaitMillisAfterError(fetchAndLock.getWaitMillisAfterError());
	}
	
	public void addVariable(String varName, Object value, String pattern) {
		if (currentVariablesNode == null) {
			currentVariablesNode = objectMapper.createObjectNode();
		}
		if (Util.isEmpty(varName)) {
			throw new IllegalArgumentException("varName cannot be null or empty");
		}
		if (value != null) {
			ObjectNode varNode = currentVariablesNode.with(varName);
			//varNode.put("type", value.getClass().getName());
			//varNode.set("valueInfo", objectMapper.createObjectNode());
			if (value instanceof Date) {
				if (Util.isEmpty(pattern)) {
					pattern = "yyyy-MM-dd'T'HH:mm:ss";
				}
				SimpleDateFormat sdf = new SimpleDateFormat(pattern);
				String strValue = sdf.format((Date) value);
				varNode.put("value", strValue);
			} else if (value instanceof JsonNode) {
				varNode.set("value", (JsonNode) value);
			} else if (value instanceof String) {
				varNode.put("value", (String) value);
			} else if (value instanceof Short) {
				varNode.put("value", (Short) value);
			} else if (value instanceof Integer) {
				varNode.put("value", (Integer) value);
			} else if (value instanceof Long) {
				varNode.put("value", (Long) value);
			} else if (value instanceof Double) {
				varNode.put("value", (Double) value);
			} else if (value instanceof Float) {
				varNode.put("value", (Float) value);
			} else if (value instanceof BigDecimal) {
				varNode.put("value", (BigDecimal) value);
			} else if (value instanceof Boolean) {
				varNode.put("value", (Boolean) value);
			} else if (value instanceof byte[]) {
				varNode.put("value", (byte[]) value);
			} else {
				varNode.put("value", value.toString());
			}
		}
	}
	
	public void complete() throws Exception {
		currentTaskExpired = false;
		workerId = fetchAndLock.getWorkerId();
		if (workerId == null) {
			throw new IllegalStateException("workerId not provided by the fetchAndLock component");
		}
		taskId = fetchAndLock.getCurrentTaskId();
		if (taskId == null) {
			throw new IllegalStateException("taskId not provided by the fetchAndLock component");
		}
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
		HttpClient client = createHttpClient();
		client.post(getExternalTaskEndpointURL() + "/" + taskId + "/complete", camundaUser, camundaPassword, requestPayload, false);
		if (client.getStatusCode() != 204) {
			String message = "Complete POST-payload: \n" + requestPayload.toString() + "\n failed: status-code: " + client.getStatusCode() + " message: " + client.getStatusMessage();
			LOG.error(message);
			throw new Exception(message);
		}
	}
	
	public void bpmnError(String errorCode) throws Exception {
		workerId = fetchAndLock.getWorkerId();
		if (workerId == null) {
			throw new IllegalStateException("workerId not provided by the fetchAndLock component");
		}
		taskId = fetchAndLock.getCurrentTaskId();
		if (taskId == null) {
			throw new IllegalStateException("taskId not provided by the fetchAndLock component");
		}
		ObjectNode requestPayload = objectMapper.createObjectNode();
		requestPayload.put("workerId", workerId);
		requestPayload.put("errorCode", errorCode);
		HttpClient client = createHttpClient();
		client.post(getExternalTaskEndpointURL() + "/" + taskId + "/bpmnError", camundaUser, camundaPassword, requestPayload, false);
		if (client.getStatusCode() != 204) {
			String message = "BpmnError POST-payload: \n" + requestPayload.toString() + "\n failed: status-code: " + client.getStatusCode() + " message: " + client.getStatusMessage();
			LOG.error(message);
			throw new Exception(message);
		}
	}

	public void failure(String errorMessage, String errorDetails, Integer retries, Number retryTimeout) throws Exception {
		workerId = fetchAndLock.getWorkerId();
		if (workerId == null) {
			throw new IllegalStateException("workerId not provided by the fetchAndLock component");
		}
		taskId = fetchAndLock.getCurrentTaskId();
		if (taskId == null) {
			throw new IllegalStateException("taskId not provided by the fetchAndLock component");
		}
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
		HttpClient client = createHttpClient();
		client.post(getExternalTaskEndpointURL() + "/" + taskId + "/failure", camundaUser, camundaPassword, requestPayload, false);
		if (client.getStatusCode() != 204) {
			String message = "Failure POST-payload: \n" + requestPayload.toString() + "\n failed: status-code: " + client.getStatusCode() + " message: " + client.getStatusMessage();
			LOG.error(message);
			throw new Exception(message);
		}
	}
	
	public void unlock() throws Exception {
		taskId = fetchAndLock.getCurrentTaskId();
		if (taskId == null) {
			throw new IllegalStateException("taskId not provided by the fetchAndLock component");
		}
		HttpClient client = createHttpClient();
		client.post(getExternalTaskEndpointURL() + "/" + taskId + "/unlock", camundaUser, camundaPassword, null, false);
		if (client.getStatusCode() != 204) {
			String message = "Unlock POST failed: status-code: " + client.getStatusCode() + " message: " + client.getStatusMessage();
			LOG.error(message);
			throw new Exception(message);
		}
	}

	public String getTaskId() {
		return taskId;
	}

	public void setTaskId(String taskId) {
		if (Util.isEmpty(taskId)) {
			throw new IllegalArgumentException("taskId cannot be null or empty");
		}
		this.taskId = taskId;
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

}

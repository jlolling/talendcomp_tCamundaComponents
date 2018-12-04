package de.jlo.talendcomp.camunda.externaltask;

import java.lang.management.ManagementFactory;
import java.math.BigDecimal;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import de.jlo.talendcomp.camunda.CamundaClient;
import de.jlo.talendcomp.camunda.HttpClient;
import de.jlo.talendcomp.camunda.TypeUtil;
import de.jlo.talendcomp.camunda.Util;
import de.jlo.talendcomp.camunda.jmx.CamundaExtTaskInfo;

public class FetchAndLock extends CamundaClient {
	
	private List<String> requestedVariables = new ArrayList<String>();
	private int numberTaskToFetch = 1;
	private ArrayNode fetchedTaskArray = null;
	private ObjectNode currentRawTask = null;
	private int currentTaskIndex = 0;
	private long timeBetweenFetches = 1;
	private long startTime = 0;
	private long stopTime = 0;
	private long currentTaskStartTime = 0;
	protected long lockDuration = 1000l;
	private long numberFetches = 0;
	private int numberSucessfulFetches = 0;
	private int numberTasksReceived = 0;
	private boolean firstFetch = true;
	protected String workerId = null;
	protected String topicName = null;
	private boolean usePriority = false;
	private boolean returnAllTasksCurrentlyFetched = false;
	private boolean deserializeFetchedJsonValues = false;
	private boolean reuseHttpClientForAllRequests = false;
	private long taskProcessingStartTime = 0l;
	
	public FetchAndLock() {
		startTime = System.currentTimeMillis();
	}
	
	private ArrayNode getVariableNames() {
		if (requestedVariables.isEmpty()) {
			throw new IllegalStateException("No topic variables available, at least one is mandatory!");
		}
		ArrayNode an = objectMapper.createArrayNode();
		for (String var : requestedVariables) {
			an.add(var);
		}
		return an;
	}
	
	public int fetchAndLock() throws Exception {
		if (workerId == null) {
			throw new IllegalStateException("initialize failed: workerId is not set");
		}
		ObjectNode requestPayload = objectMapper.createObjectNode();
		requestPayload.put("workerId", workerId);
		requestPayload.put("maxTasks", numberTaskToFetch);
		requestPayload.put("usePriority", usePriority);
		requestPayload
			.withArray("topics")
				.addObject()
					.put("topicName", topicName)
					.put("lockDuration", lockDuration)
					.set("variables", getVariableNames());
		currentTaskIndex = 0;
		fetchedTaskArray = null;
		currentTaskStartTime = 0;
		currentRawTask = null;
		while (true) {
			if (stopTimeReached()) {
				break;
			}
			if (firstFetch == false) {
				Thread.sleep(timeBetweenFetches);
			} else {
				firstFetch = false;
			}
			if (reuseHttpClientForAllRequests == false) {
				setHttpClient(null);
			}
			HttpClient client = getHttpClient();
			long startTime = System.currentTimeMillis();
			String responseStr = client.post(getExternalTaskEndpointURL() + "/fetchAndLock", requestPayload, true);
			int countRetries = client.getCurrentAttempt();
			long fetchDuration = System.currentTimeMillis() - startTime;
			if (mbeanCamundaExtTaskInfo != null) {
				mbeanCamundaExtTaskInfo.addFetchAndLock(fetchDuration, countRetries);
			}
//			if (client.getStatusCode() != 200) {
//				String errorMessage = "Worker " + workerId + ": FetchAndLock POST-payload: \n" + requestPayload.toString() + "\n failed: status-code: " + client.getStatusCode() + " message: " + client.getStatusMessage() + "\nResponse: " + responseStr;
//				if (mbeanCamundaExtTaskInfo != null) {
//					mbeanCamundaExtTaskInfo.addFetchAndLock(fetchDuration, countRetries);
//				}
//				throw new Exception(errorMessage);
//			} else {
//				if (mbeanCamundaExtTaskInfo != null) {
//					mbeanCamundaExtTaskInfo.addFetchAndLock(fetchDuration, countRetries);
//				}
//			}
			try {
				fetchedTaskArray = (ArrayNode) objectMapper.readTree(responseStr);
			} catch (Exception e) {
				throw e;
			}
			numberFetches++;
			if (fetchedTaskArray != null && fetchedTaskArray.size() > 0) {
				if (mbeanCamundaExtTaskInfo != null) {
					mbeanCamundaExtTaskInfo.addTaskFetched(fetchedTaskArray.size());
				}
				numberSucessfulFetches++;
				numberTasksReceived = numberTasksReceived + fetchedTaskArray.size();
				break;
			}
		}		
		if (fetchedTaskArray != null) {
			return fetchedTaskArray.size();
		} else {
			return 0;
		}
	}
	
	public void addVariable(String var) {
		if (requestedVariables.contains(var) == false) {
			requestedVariables.add(var);
		}
	}
	
	private boolean stopTimeReached() {
		if (startTime > 0l && stopTime > 0l) {
			// check the runtime but take care we are trying fetch at at least one time
			long currentTime = System.currentTimeMillis();
			if (currentTime >= stopTime) {
				System.out.println("Worker: " + workerId + ": Stop fetching tasks because max runtime is reached.");
				return true;
			}
		}
		return false;
	}
	
	public ObjectNode getCurrentTask() throws Exception {
		return getSimplifiedTask(currentRawTask);
	}
	
	public ObjectNode getSimplifiedTask(JsonNode rawTaskNode) throws Exception {
		ObjectNode taskNode = objectMapper.createObjectNode();
		String id = getTaskId(rawTaskNode);
		taskNode.put("taskId", id);
		taskNode.put("id", id);
		taskNode.put("processInstanceId", getTaskProcessInstanceId(rawTaskNode));
		taskNode.set("lockExpirationTime", rawTaskNode.get("lockExpirationTime"));
		taskNode.set("activityId", rawTaskNode.get("activityId"));
		taskNode.set("activityInstanceId", rawTaskNode.get("activityInstanceId"));
		taskNode.set("errorMessage", rawTaskNode.get("errorMessage"));
		taskNode.set("errorDetails", rawTaskNode.get("errorDetails"));
		taskNode.set("executionId", rawTaskNode.get("executionId"));
		taskNode.set("processDefinitionId", rawTaskNode.get("processDefinitionId"));
		taskNode.set("processDefinitionKey", rawTaskNode.get("processDefinitionKey"));
		taskNode.set("tenantId", rawTaskNode.get("tenantId"));
		taskNode.set("retries", rawTaskNode.get("retries"));
		taskNode.set("suspended", rawTaskNode.get("suspended"));
		taskNode.set("workerId", rawTaskNode.get("workerId"));
		taskNode.set("priority", rawTaskNode.get("priority"));
		taskNode.set("topicName", rawTaskNode.get("topicName"));
		for (String varName : requestedVariables) {
			JsonNode valueNode = getTaskVariableValueNode(rawTaskNode, varName, true, true);
			if (valueNode != null && valueNode.isMissingNode() == false) {
				taskNode.set(varName, valueNode);
			}
		}
		return taskNode;
	}
	
	public ArrayNode getTasksCurrentlyFetched() throws Exception {
		ArrayNode an = objectMapper.createArrayNode();
		// iterate through the task and set the variables
		if (fetchedTaskArray != null) {
			for (JsonNode oneFetchedTask : fetchedTaskArray) {
				an.add(getSimplifiedTask(oneFetchedTask));
			}
		}
		return an;
	}
	
	public boolean nextFetchAndLock() throws Exception {
		returnAllTasksCurrentlyFetched = true;
		if (Thread.currentThread().isInterrupted()) {
			return false;
		}
		fetchAndLock();
		currentTaskStartTime = System.currentTimeMillis();
		if (fetchedTaskArray != null && fetchedTaskArray.size() > 0) {
			taskProcessingStartTime = System.currentTimeMillis();
			return true;
		} else {
			return false;
		}
	}

	public boolean nextTask() throws Exception {
		currentRawTask = null;
		if (Thread.currentThread().isInterrupted()) {
			return false;
		}
		if (fetchedTaskArray == null || currentTaskIndex == fetchedTaskArray.size()) {
			fetchAndLock();
		}
		currentTaskStartTime = System.currentTimeMillis();
		if (fetchedTaskArray != null) {
			if (currentTaskIndex < fetchedTaskArray.size()) {
				currentRawTask = (ObjectNode) fetchedTaskArray.get(currentTaskIndex++);
				return true;
			}
		}
		return false;
	}
	
	/**
	 * Index of the current task received with the last fetch
	 * @return 1-based index
	 */
	public int getCurrentTaskIndexCurrentlyFetched() {
		return currentTaskIndex;
	}
	
	/**
	 * Number of tasks currently fetched
	 * @return number - will be reseted in every fetch
	 */
	public int getCountTasksCurrentlyFetched() {
		if (fetchedTaskArray != null) {
			return fetchedTaskArray.size();
		} else {
			return 0;
		}
	}
	
	/**
	 * Indicates the current task is the last one received by the last fetch
	 * @return true if task is the last
	 */
	public boolean isLastTaskCurrentlyFetched() {
		if (fetchedTaskArray != null) {
			return currentTaskIndex == fetchedTaskArray.size();
		} else {
			return true;
		}
	}
	
	public String getCurrentTaskId() {
		if (currentRawTask == null || currentRawTask.isNull()) {
			throw new IllegalStateException("Current task not fetched");
		}
		return getTaskId(currentRawTask);
	}
	
	public String getTaskId(JsonNode taskNode) {
		if (taskNode == null || taskNode.isNull()) {
			throw new IllegalStateException("Task node not be null or a NullNode");
		}
		String id = taskNode.get("id").asText();
		if (Util.isEmpty(id)) {
			throw new IllegalStateException("Task without id received. Current task: " + taskNode.toString());
		}
		return id;
	}

	public Date getCurrentTaskLockExpirationTime() throws Exception {
		if (currentRawTask == null || currentRawTask.isNull()) {
			throw new IllegalStateException("Current task not fetched");
		}
		return getTaskLockExpirationTime(currentRawTask);
	}

	public Date getTaskLockExpirationTime(JsonNode taskNode) throws Exception {
		if (taskNode == null || taskNode.isNull()) {
			throw new IllegalStateException("Task node cannot be null or a NullNode");
		}
		String s = taskNode.path("lockExpirationTime").asText();
		if (s != null && s.trim().isEmpty() == false) {
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
			Date time = null;
			try {
				time = sdf.parse(s);
			} catch (ParseException e) {
				return null;
			}
			return time;
		} else {
			return null;
		}
	}

	public String getCurrentTaskProcessInstanceId() {
		if (currentRawTask == null || currentRawTask.isNull()) {
			throw new IllegalStateException("Current task not fetched");
		}
		return getTaskProcessInstanceId(currentRawTask);
	}
	
	public String getTaskProcessInstanceId(JsonNode taskNode) {
		if (taskNode == null || taskNode.isNull()) {
			throw new IllegalStateException("Task node cannot be null or a NullNode");
		}
		String id = taskNode.get("processInstanceId").asText();
		if (Util.isEmpty(id)) {
			throw new IllegalStateException("Task without processInstanceId received. Task: " + taskNode.toString());
		}
		return id;
	}
	
	public Integer getCurrentTaskRetries() {
		if (currentRawTask == null || currentRawTask.isNull()) {
			throw new IllegalStateException("Current task not fetched");
		}
		return getTaskRetries(currentRawTask);
	}
	
	public Integer getTaskRetries(JsonNode taskNode) {
		if (taskNode == null || taskNode.isNull()) {
			throw new IllegalStateException("Task node cannot be null or a NullNode");
		}
		return taskNode.get("retries").asInt();
	}
	
	private JsonNode getTaskVariableValueNode(JsonNode taskNode, String varName, boolean missingAllowed, boolean nullable) throws Exception {
		if (Util.isEmpty(varName)) {
			throw new IllegalArgumentException("Variable name cannot be null or empty");
		}
		if (taskNode == null || taskNode.isNull()) {
			throw new IllegalStateException("Task node cannot be null or a NullNode");
		}
		JsonNode variablesNode = taskNode.get("variables");
		if (variablesNode instanceof ObjectNode) {
			JsonNode oneVarNode = variablesNode.path(varName);
			JsonNode valueNode = oneVarNode.path("value");
			if (missingAllowed == false && valueNode.isMissingNode()) {
				throw new Exception("The variable: " + varName + " is missing. Variables node: " + variablesNode.toString());
			}
			if (valueNode.isNull() || valueNode.isMissingNode()) {
				if (nullable == false) {
					throw new Exception("The variable: " + varName + " is null. Variables node: " + variablesNode.toString());
				}
				return valueNode;
			} else {
				JsonNode typeNode = oneVarNode.path("type");
				if (deserializeFetchedJsonValues && "json".equalsIgnoreCase(typeNode.asText())) {
					return objectMapper.readTree(valueNode.asText());
				} else {
					return valueNode;
				}
			}
		} else {
			throw new IllegalStateException("Task without variables received. Task: " + taskNode.toString());
		}
	}
	
	private JsonNode getCurrentTaskVariableValueNode(String varName, boolean missingAllowed, boolean nullable) throws Exception {
		if (currentRawTask == null || currentRawTask.isNull()) {
			throw new IllegalStateException("Current task not fetched");
		}
		return getTaskVariableValueNode(currentRawTask, varName, missingAllowed, nullable);
	}

	public JsonNode getCurrentTaskVariableValueAsObject(String varName, boolean missingAllowed, boolean nullable) throws Exception {
		JsonNode valueNode = getCurrentTaskVariableValueNode(varName, missingAllowed, nullable);
		if (valueNode != null) {
			String nodeStr = valueNode.asText();
			try {
				return objectMapper.readTree(nodeStr);
			} catch (Exception e) {
				String message = "Parse variable: " + varName + " as JSON failed: " + e.getMessage() + " content: " + nodeStr;
				throw new Exception(message, e);
			}
		} else {
			return null;
		}
	}

	public String getCurrentTaskVariableValueAsString(String varName, boolean missingAllowed, boolean nullable) throws Exception {
		JsonNode valueNode = getCurrentTaskVariableValueNode(varName, missingAllowed, nullable);
		if (valueNode != null) {
			return valueNode.asText();
		} else {
			return null;
		}
	}

	public Short getCurrentTaskVariableValueAsShort(String varName, boolean missingAllowed, boolean nullable) throws Exception {
		JsonNode valueNode = getCurrentTaskVariableValueNode(varName, missingAllowed, nullable);
		if (valueNode != null) {
			try {
				return TypeUtil.convertToShort(valueNode);
			} catch (Exception e) {
				String message = "Convert variable: " + varName + " to Short failed: " + e.getMessage() + " content: " + valueNode;
				throw new Exception(message, e);
			}
		} else {
			return null;
		}
	}

	public Integer getCurrentTaskVariableValueAsInteger(String varName, boolean missingAllowed, boolean nullable) throws Exception {
		JsonNode valueNode = getCurrentTaskVariableValueNode(varName, missingAllowed, nullable);
		if (valueNode != null) {
			try {
				return TypeUtil.convertToInteger(valueNode);
			} catch (Exception e) {
				String message = "Convert for variable: " + varName + " to Integer failed: " + e.getMessage() + " content: " + valueNode;
				throw new Exception(message, e);
			}
		} else {
			return null;
		}
	}

	public Long getCurrentTaskVariableValueAsLong(String varName, boolean missingAllowed, boolean nullable) throws Exception {
		JsonNode valueNode = getCurrentTaskVariableValueNode(varName, missingAllowed, nullable);
		if (valueNode != null) {
			try {
				return TypeUtil.convertToLong(valueNode);
			} catch (Exception e) {
				String message = "Convert for variable: " + varName + " to Long failed: " + e.getMessage() + " content: " + valueNode;
				throw new Exception(message, e);
			}
		} else {
			return null;
		}
	}

	public Double getCurrentTaskVariableValueAsDouble(String varName, boolean missingAllowed, boolean nullable) throws Exception {
		JsonNode valueNode = getCurrentTaskVariableValueNode(varName, missingAllowed, nullable);
		if (valueNode != null) {
			try {
				return TypeUtil.convertToDouble(valueNode);
			} catch (Exception e) {
				String message = "Convert for variable: " + varName + " to Double failed: " + e.getMessage() + " content: " + valueNode;
				throw new Exception(message, e);
			}
		} else {
			return null;
		}
	}

	public Float getCurrentTaskVariableValueAsFloat(String varName, boolean missingAllowed, boolean nullable) throws Exception {
		JsonNode valueNode = getCurrentTaskVariableValueNode(varName, missingAllowed, nullable);
		if (valueNode != null) {
			try {
				return TypeUtil.convertToFloat(valueNode);
			} catch (Exception e) {
				String message = "Convert for variable: " + varName + " to Float failed: " + e.getMessage() + " content: " + valueNode;
				throw new Exception(message, e);
			}
		} else {
			return null;
		}
	}

	public BigDecimal getCurrentTaskVariableValueAsBigDecimal(String varName, boolean missingAllowed, boolean nullable) throws Exception {
		JsonNode valueNode = getCurrentTaskVariableValueNode(varName, missingAllowed, nullable);
		if (valueNode != null) {
			try {
				return TypeUtil.convertToBigDecimal(valueNode);
			} catch (Exception e) {
				String message = "Convert for variable: " + varName + " to BigDecimal failed: " + e.getMessage() + " content: " + valueNode;
				throw new Exception(message, e);
			}
		} else {
			return null;
		}
	}

	public Boolean getCurrentTaskVariableValueAsBoolean(String varName, boolean missingAllowed, boolean nullable) throws Exception {
		JsonNode valueNode = getCurrentTaskVariableValueNode(varName, missingAllowed, nullable);
		if (valueNode != null) {
			try {
				return TypeUtil.convertToBoolean(valueNode);
			} catch (Exception e) {
				String message = "Convert for variable: " + varName + " to Boolean failed: " + e.getMessage() + " content: " + valueNode;
				throw new Exception(message, e);
			}
		} else {
			return null;
		}
	}

	public Date getCurrentTaskVariableValueAsDate(String varName, String pattern, boolean missingAllowed, boolean nullable) throws Exception {
		JsonNode valueNode = getCurrentTaskVariableValueNode(varName, missingAllowed, nullable);
		if (valueNode != null) {
			try {
				return parseDate(valueNode.asText(), pattern);
			} catch (Exception e) {
				String message = "Convert for variable: " + varName + " to Date failed: " + e.getMessage() + " content: " + valueNode;
				throw new Exception(message, e);
			}
		} else {
			return null;
		}
	}

	public long getTimeBetweenFetches() {
		return timeBetweenFetches;
	}

	public void setTimeBetweenFetches(Integer timeBetweenFetches) {
		if (timeBetweenFetches != null && timeBetweenFetches >= 1) {
			this.timeBetweenFetches = timeBetweenFetches.longValue();
		}
	}

	public void setTimeBetweenFetches(Long timeBetweenFetches) {
		if (timeBetweenFetches != null && timeBetweenFetches >= 1l) {
			this.timeBetweenFetches = timeBetweenFetches.longValue();
		}
	}

	public long getStopTime() {
		return stopTime;
	}

	public void setStopTime(Integer secondsFetching) {
		if (secondsFetching != null && secondsFetching > 0) {
			this.stopTime = System.currentTimeMillis() + 10l + (secondsFetching * 1000l);
		}
	}

	public void setStopTime(Date stopDate) {
		if (stopDate != null) {
			this.stopTime = stopDate.getTime() + 10l;
		}
	}

	public int getNumberTaskToFetch() {
		return numberTaskToFetch;
	}

	public void setNumberTaskToFetch(Integer numberTaskToFetch) {
		if (numberTaskToFetch != null) {
			this.numberTaskToFetch = numberTaskToFetch;
		}
	}

	public void setLockDuration(Long lockDuration) {
		if (lockDuration != null) {
			this.lockDuration = lockDuration.longValue();
		}
	}
	
	public void setLockDuration(Integer lockDuration) {
		if (lockDuration != null) {
			this.lockDuration = lockDuration.longValue();
		}
	}

	public long getLockDuration() {
		return lockDuration;
	}

	public ObjectNode getCurrentRawTask() {
		return currentRawTask;
	}

	public long getNumberFetches() {
		return numberFetches;
	}

	public int getNumberSucessfulFetches() {
		return numberSucessfulFetches;
	}

	public int getNumberTasksReceived() {
		return numberTasksReceived;
	}

	public String getWorkerId() {
		return workerId;
	}

	public void setWorkerId(String workerId) {
		this.workerId = workerId;
	}

	public void setTopicName(String topicName) {
		if (Util.isEmpty(topicName)) {
			throw new IllegalArgumentException("Topic name cannot be null or empty");
		}
		this.topicName = topicName;
	}

	public String getTopicName() {
		return topicName;
	}

	public boolean isUsePriority() {
		return usePriority;
	}

	public void setUsePriority(Boolean usePriority) {
		if (usePriority != null) {
			this.usePriority = usePriority.booleanValue();
		}
	}

	public long getCurrentTaskStartTime() {
		return currentTaskStartTime;
	}

	public boolean isReturnAllTasksCurrentlyFetched() {
		return returnAllTasksCurrentlyFetched;
	}

	public boolean isDeserializeFetchedJsonValues() {
		return deserializeFetchedJsonValues;
	}

	public void setDeserializeFetchedJsonValues(Boolean deserializeFetchedJsonValues) {
		if (deserializeFetchedJsonValues != null) {		
			this.deserializeFetchedJsonValues = deserializeFetchedJsonValues.booleanValue();
		}
	}

	public boolean isReuseHttpClientForAllRequests() {
		return reuseHttpClientForAllRequests;
	}

	public void setReuseHttpClientForAllRequests(Boolean reuseHttpClientForAllRequests) {
		if (reuseHttpClientForAllRequests != null) {
			this.reuseHttpClientForAllRequests = reuseHttpClientForAllRequests;
		}
	}
	
	private ObjectName beanName = null;

	public void registerMBean() throws Exception {
		mbeanCamundaExtTaskInfo = new CamundaExtTaskInfo();
		mbeanCamundaExtTaskInfo.setFetchAndLock(this);
		MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
		beanName = new ObjectName("de.jlo.talendcomp.camundExtTask:type="+CamundaExtTaskInfo.class.getSimpleName() + ",topic=" + topicName + ",workerId=" + workerId);
		if (mbs.isRegistered(beanName) == false) {
			mbs.registerMBean(mbeanCamundaExtTaskInfo, beanName);
		}
	}
	
	public void unregisterMBean() {
		if (mbeanCamundaExtTaskInfo != null) {
			mbeanCamundaExtTaskInfo.stop();
			mbeanCamundaExtTaskInfo.setFetchAndLock(null);
		}
		if (beanName != null) {
			MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
			try {
				mbs.unregisterMBean(beanName);
			} catch (Exception e) {
				System.err.println("Unable to unregister mbean: " + e.getMessage());
			}
		}
	}
	
	public CamundaExtTaskInfo getCamundaExtTaskInfo() {
		return mbeanCamundaExtTaskInfo;
	}
	
	@Override
	public void close() {
		unregisterMBean();
		super.close();
	}

	public long getTaskProcessingStartTime() {
		return taskProcessingStartTime;
	}
	
}

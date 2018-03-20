package de.jlo.talendcomp.camunda.externaltask;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.camunda.bpm.engine.ExternalTaskService;
import org.camunda.bpm.engine.externaltask.LockedExternalTask;

public class Client {
	
	private ExternalTaskService externalTaskService = null;
	private List<Topic> topicList = new ArrayList<Topic>();
	private List<LockedExternalTask> tasks = null;
	private int numberTaskToFetch = 1;
	private long lockDuration = 1000l;
	private String workerId = null;
	private String camundaHost = null;
	private int camundaPort = 8080;
	private String camundaUser = null;
	private String camundaPassword = null;

	public void addTopic(String name, Long lockDuration) {
		Topic t = new Topic(name, lockDuration);
		if (topicList.contains(t)) {
			throw new IllegalArgumentException("Topic " + t + " already configured");
		} else {
			topicList.add(t);
		}
	}

	public int getNumberTaskToFetch() {
		return numberTaskToFetch;
	}

	public void setNumberTaskToFetch(int numberTaskToFetch) {
		this.numberTaskToFetch = numberTaskToFetch;
	}

	public String getWorkerId() {
		return workerId;
	}

	public void setWorkerId(String workerId) {
		this.workerId = workerId;
	}

	public long getLockDuration() {
		return lockDuration;
	}

	public String getCamundaHost() {
		return camundaHost;
	}

	public void setCamundaHost(String camundaHost) {
		this.camundaHost = camundaHost;
	}

	public int getCamundaPort() {
		return camundaPort;
	}

	public void setCamundaPort(int camundaPort) {
		this.camundaPort = camundaPort;
	}

	public String getCamundaUser() {
		return camundaUser;
	}

	public void setCamundaUser(String camundaUser) {
		this.camundaUser = camundaUser;
	}

	public String getCamundaPassword() {
		return camundaPassword;
	}

	public void setCamundaPassword(String camundaPassword) {
		this.camundaPassword = camundaPassword;
	}

	public void initialize() throws Exception {
		if (workerId == null) {
			throw new IllegalStateException("initialize failed: workerId is not set");
		}
		if (camundaHost == null) {
			throw new IllegalStateException("initialize failed: camundaHost is not set");
		}
		
	}
	
	public int fetchAndLock() throws Exception {
		if (externalTaskService == null) {
			throw new IllegalStateException("client not initialized");
		}
		return 0;
	}
	
	public void complete(String taskId, Map<String, Object> variables) throws Exception {
		
	}
	
	public void failure(String taskId, String errorMessage) throws Exception {
		
	}
	
	public void unlock(String taskId) throws Exception {
		
	}
	
}

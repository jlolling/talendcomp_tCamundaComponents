package de.jlo.talendcomp.camunda.externaltask;

public class Topic {
	
	private String name = null;
	private long lockDuration = 10000l;
	
	public Topic(String name, Long lockDuration) {
		if (name == null || name.trim().isEmpty()) {
			throw new IllegalArgumentException("Topic name cannot be null or empty");
		}
		this.name = name;
		if (lockDuration != null) {
			this.lockDuration = lockDuration;
		}
	}

	public Topic(String name) {
		this(name, 10000l);
	}

	public String getName() {
		return name;
	}

	public long getLockDuration() {
		return lockDuration;
	}
	
	@Override
	public boolean equals(Object o) {
		if (o instanceof Topic) {
			return this.name.equals(((Topic) o).name);
		} else {
			return false;
		}
	}
	
	@Override
	public String toString() {
		return name;
	}
	
}
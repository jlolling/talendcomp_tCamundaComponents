package de.jlo.talendcomp.camunda.externaltask;

import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.BasicConfigurator;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class CamundaClientTest {
	
	private Map<String, Object> globalMap = new HashMap<String, Object>();

	@Before
	public void init() {
		BasicConfigurator.configure();
	}
	
	@Test
	public void testFetchAndLock() throws Exception {
		FetchAndLock cc = new FetchAndLock();
		globalMap.put("comp1", cc);
		cc.setCamundaServiceURL("http://camundatest02.gvl.local:10080");
		cc.setCamundaUser("talend_test");
		cc.setCamundaPassword("talend_test");
		cc.setWorkerId("camundaClientTest");
		cc.setNumberTaskToFetch(1);
		cc.setMaxRetriesInCaseOfErrors(4);
		cc.setTopicName("calculateConflict");
		cc.addVariable("calculateConflict");
		cc.setLockDuration(1000);
		cc.setStopTime(60);
		while (cc.next()) {
			System.out.println(cc.getCurrentTaskId());
			System.out.println(cc.getCurrentTaskVariableValueAsObject("calculateConflict", false, false));
		}
		Assert.assertTrue(true);

	}

}

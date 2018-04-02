package de.jlo.talendcomp.camunda.externaltask;

import org.apache.log4j.BasicConfigurator;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class CamundaClientTest {

	@Before
	public void init() {
		BasicConfigurator.configure();
	}
	
	@Test
	public void testFetchAndLock() throws Exception {
		FetchAndLock cc = new FetchAndLock();
		cc.setCamundaServiceURL("http://camundatest02.gvl.local:10080");
		cc.setCamundaUser("talend_test");
		cc.setCamundaPassword("talend_test");
		cc.setWorkerId("camundaClientTest");
		cc.setNumberTaskToFetch(1);
		cc.setMaxRetriesInCaseOfErrors(4);
		cc.setTopicName("calculateConflict");
		cc.addVariable("calculateConflict");
		cc.setLockDuration(10);
		cc.setStopTime(5);
		while (cc.next()) {
			System.out.println(cc.getCurrentTaskId());
			System.out.println(cc.getCurrentTaskVariableValueAsObject("calculateConflict"));
		}
		Assert.assertTrue(true);
	}

}

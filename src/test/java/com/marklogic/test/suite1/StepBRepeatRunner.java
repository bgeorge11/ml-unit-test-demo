package com.marklogic.test.suite1;

import static org.junit.Assert.assertEquals;

import java.util.List;

import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.ParallelComputer;
import org.junit.runner.JUnitCore;
import org.junit.runner.Result;
import org.junit.runner.RunWith;
import org.junit.runner.notification.Failure;
import org.junit.runners.Parameterized;

public class StepBRepeatRunner {
	
	@Rule
	public RepeatRule repeatRule = new RepeatRule();
	
	@Repeat(100)
	@Test
	public void testAllDataOperations() {
		Class[] cls = { DataOperationsTest0.class, 
				        DataOperationsTest1.class, 
				        DataOperationsTest2.class,
				        BinaryLoadTest1.class};

		Result results = JUnitCore.runClasses(ParallelComputer.methods(), cls);

		// System.out.println("-------------DATA OPERATIONS TEST
		// RESULTS---------------");
		// System.out.println("Total RunCount = " + results.getRunCount());
		// System.out.println("Failure Count = " + results.getFailureCount());
		// System.out.println("Run Time = " + results.getRunTime() / 1000 + "
		// seconds");
		List<Failure> failure = results.getFailures();

		for (int i = 0; i < failure.size(); i++) {
			System.out.println("Data Operations Failure details " + failure.get(i).getTestHeader() + " "
					+ failure.get(i).getException());
		}

		assertEquals(0, results.getFailureCount());
	}
}

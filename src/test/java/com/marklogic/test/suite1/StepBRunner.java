package com.marklogic.test.suite1;

import static org.junit.Assert.assertEquals;

import java.util.List;

import org.junit.Test;
import org.junit.experimental.ParallelComputer;
import org.junit.runner.JUnitCore;
import org.junit.runner.Result;
import org.junit.runner.RunWith;
import org.junit.runner.notification.Failure;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class StepBRunner {

	@Parameterized.Parameters
	public static Object[][] data() {
		return new Object[1][0];
	}

	@Test
	public void testAllDataOperations() {
		Class[] cls = { DataOperationsTest0.class, DataOperationsTest1.class, DataOperationsTest2.class };

		Result results = JUnitCore.runClasses(ParallelComputer.classes(), cls);

//		System.out.println("-------------DATA OPERATIONS TEST RESULTS---------------");
//		System.out.println("Total RunCount = " + results.getRunCount());
//		System.out.println("Failure Count = " + results.getFailureCount());
//		System.out.println("Run Time = " + results.getRunTime() / 1000 + " seconds");
		List<Failure> failure = results.getFailures();

		for (int i = 0; i < failure.size(); i++) {
			System.out
					.println("Data Operations Failure details " + failure.get(i).getTestHeader() + " " + failure.get(i).getException());
		}

		assertEquals(0, results.getFailureCount());
	}
}

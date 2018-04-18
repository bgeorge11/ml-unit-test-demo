package com.marklogic.test.suite1;

import static org.junit.Assert.assertEquals;

import java.util.Date;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.ParallelComputer;
import org.junit.runner.JUnitCore;
import org.junit.runner.Result;
import org.junit.runner.notification.Failure;

public class TestSuiteA {
	
	private Date startTime ;
	private Date endTime ;

	@Before
	public void setUpDatabase() {
		Class[] cls = { StepARunner.class };
		
		startTime = new Date();

		// Parallel among methods
		Result results = JUnitCore.runClasses(ParallelComputer.classes(), cls);

		System.out.println("-------------DATABASE AND FORESTS CREATED, FORESTS ATTACHED RESULTS ---------------");
		System.out.println("Total RunCount = " + results.getRunCount());
		System.out.println("Failure Count = " + results.getFailureCount());
		System.out.println("Run Time = " + results.getRunTime() / 1000 + " seconds");
		List<Failure> failure = results.getFailures();

		for (int i = 0; i < failure.size(); i++) {
			System.out
					.println("Failure details " + failure.get(i).getTestHeader() + " " + failure.get(i).getException());
		}

		assertEquals(0, results.getFailureCount());

	}

	@Test
	public void testDatabaseOperations() {
		Class[] cls = { StepBRunner.class };

		// Parallel among methods
		Result results = JUnitCore.runClasses(ParallelComputer.classes(), cls);

		System.out.println("-------------DATA OPERATIONS TEST RESULTS---------------");
		System.out.println("Total RunCount = " + results.getRunCount());
		System.out.println("Failure Count = " + results.getFailureCount());
		System.out.println("Run Time = " + results.getRunTime() / 1000 + " seconds");
		List<Failure> failure = results.getFailures();

		for (int i = 0; i < failure.size(); i++) {
			System.out.println("DATABASE OPERATIONS Failure details " + failure.get(i).getTestHeader() + " "
					+ failure.get(i).getException());
		}

		assertEquals(0, results.getFailureCount());

	}

	@After
	public void tearDownDatabase() {
		Class[] cls = { StepCRunner.class };
		
		// Parallel among methods
		Result results = JUnitCore.runClasses(ParallelComputer.classes(), cls);

		System.out.println("-------------TEAR DOWN TEST RESULTS---------------");
		System.out.println("Total RunCount = " + results.getRunCount());
		System.out.println("Failure Count = " + results.getFailureCount());
		System.out.println("Run Time = " + results.getRunTime() / 1000 + " seconds");
		List<Failure> failure = results.getFailures();

		for (int i = 0; i < failure.size(); i++) {
			System.out.println("TEAR DOWN Failure details " + failure.get(i).getTestHeader() + " "
					+ failure.get(i).getException());
		}

		assertEquals(0, results.getFailureCount());
		endTime = new Date();
		
		System.out.println("TOTAL TIME OF EXECUTION = " + 
		                   (endTime.getTime() - startTime.getTime())/1000 + " seconds..");

	}
}

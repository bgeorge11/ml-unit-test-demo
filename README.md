# MarkLogic Unit Testing

This project demostrates the usage of MarkLogic Management and CRUD APIs and integrate with Junit. 

# Usage

1. Import the project as MAVEN project. All dependencies are in pom,xml. 
2. Verify the default entries in suite1.properties and user.properties in src\test\resources
3. Build the maven project as <br>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; <i> >mvn clean install -DskipTests=true </i>  (Note the skipTests so that tests are not automatically done) 
 4. Run the project as a Junit test project  <br>
 &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; <i> >mvn -Dtest=TestSuiteA test </i> <br>
      TestSuiteA runs the below  <br>
     &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; StepARunner --> Creates the databases and forests. Also attaches the forests to databases. The number and name of databases are controlled by <i>suite1.properties</i> <br>
     &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; StepBRunner --> Executes the CRUD Operations test in parallel. The path to the files to upload are in <i>DataOperations.properties</i> <br>
     &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; StepCRunner --> Deletes the databases and forests created.  <br>
     All the tests can be independently run as well. 
# How to add new testcases 
1. Refer one of the existing test case 
2. Create a new test case file with the file name indicating the database to be used and matching one of the patterns in <i>testClassPatterns</i> property in <i>suite1.properties</i>. For example, if <i>testClassPatterns</i> has <i>LoadTest</i>, a test case file name can be <i>JSONDocumentLoadTest12.java</i>. This will ensure that the testing is done against database created with index number 12. 
<i>Note:</i> This is one way of linking test cases to databases. The test case java file can have their own mechanism to work against a database that was created in StepA. 
3. To execute the testing as part of StepB, include new test file in <i>StepBRunner.java </i> in method <i>testAllDataOperations</i><br>
<i>Note:</i> The package uses surefire plugin and is configured to run all test classes parallely. So, it is assumed that the test cases are independent. 
 
 
# Other information 
1. The package is verified with JDK 1.8 
2. There is one demo test case (<i>DataOperationsTest0</i>) for MarkLogic Content Pump (mlcp). mlcp being a command line tool, please ensure that it is available in PATH. If there is no mlcp, do not execute that test. To skip the test place <i>@Ignore</i> annotation at the class level. 

# TestSuiteB vs TestSuiteA 
TestSuiteB does not delete the databases created. The documents loaded will remain in the databases and can be verified if required. Make sure that databases and later manually deleted or execute test <i>StepCRunner</i> to tear down the databases. Commands to run TestSuiteB <br>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; >mvn -Dtest=TestSuiteB test


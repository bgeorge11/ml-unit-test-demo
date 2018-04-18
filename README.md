# MarkLogic Unit Testing

This project demostrates the usage of MarkLogic Management and CRUD APIs and integrate with Junit. 


# Usage

1. Import the project as MAVEN project. All dependencies are in pom,xml. 
2. Verify the default entries in suite1.properties and user.properties in src\test\resources
3. Build the maven project as 
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; <i> >mvn clean install -DskipTests=true <i>  (Note the skipTests so that tests are not automatically done) 
 4. Run the project as a Junit test project 
 &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; <i> >mvn -Dtest=TestSuiteA test </i>
      TestSuiteA runs the below 
     &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; StepARunner --> Creates the databases and forests. Also attaches the forests to databases
     &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; StepBRunner --> Executes the CRUD Operations test in parallel 
     &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; StepCRunner --> Deletes the databases and forests created. 
     All the tests can be independently run as well. 
# How to add new testcases 
1. Refer one of the existing test case 
2. Create a new test case file with the file name indicating the database to be used and matching one of the patterns in <i>testClassPatterns</i> property in <i>suite1.properties</i>. For example, if <i>testClassPatterns</i> has <i>LoadTest</i>, a test case file name can be <i>JSONDocumentLoadTest12.java</i>. This will ensure that the testing is done against database created with index number 12. 
<i>Note:</i> This is one way of linking test cases to databases. The test case java file can have their own mechanism to work against a database that was created in StepA. 
3. To execute the testing as part of StepB, include new test file in <i>StepBRunner.java </i> in method <i>testAllDataOperations</i>
<i>Note:</i> The package uses surefire plugin and is configured to run all test classes parallely. So, it is assumed that the test cases are independent. 



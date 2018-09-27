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
3. To execute the testing as part of StepB, include new test file in <i>StepBRunner.java</i> and <i>StepBRepeatRunner.java </i> in method <i>testAllDataOperations</i><br>
<i>Note:</i> The package uses surefire plugin and is configured to run all test classes parallely. So, it is assumed that the test cases are independent. 
 
 
# Other information 
1. The package is verified with JDK 1.8 
2. There is one demo test case (<i>DataOperationsTest0</i>) for MarkLogic Content Pump (mlcp). mlcp being a command line tool, please ensure that it is available in PATH. If there is no mlcp, do not execute that test. To skip the test place <i>@Ignore</i> annotation at the class level. 
3. If the number of databases need to be only the number of test classes, use the property value <i>numTestCases=0</i> in <i>suite1.properties</i> file. Then the number of databases created will be determined by the number of test classes with class names matching the patterns in <i>testClassPatterns</i>

# TestSuiteA
TestSuiteA creates databases and forests, performs the data operations tests and tears down all the databases and forests. TestSuiteA can be repeated without any other steps in between because if the tests are completed all the objects created are deleted. Command to run TestSuiteA  <br>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; <i>mvn -Dtest=TestSuiteA test </i>

# TestSuiteB
TestSuiteB does not delete the databases created. Otherwise, it is same as TestSuiteA. The documents loaded will remain in the databases and can be verified if required. Make sure that databases and later manually deleted or execute test <i>StepCRunner</i> to tear down the databases. Commands to run TestSuiteB <br>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; <i>mvn -Dtest=TestSuiteB test </i>

# TestSuiteC
TestSuiteC can be used for repeating the data operations (StepB) as many times mentioned in <i>@Repeat</i> annotation. The databases are created once and deleted once after all the tests. Commands to run TestSuiteC <br>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; <i>mvn -Dtest=TestSuiteC test </i>
StepBRepeatRunner is provided separately to configure classes which needs to be tested repeatedly (as for doing a performance benchmark)

# List of TestCases 
1. MLCPDataOperationsTest1.java <br>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;testImportDelimitedTextWithTransformation --> Loads a CSV as JSON with transformation and uses mlcp. The transformation modules are installed as part of the test and deleted at the end of test. <br>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;testImportDelimitedText --> Loads the same CSV as XML without transformation and uses mlcp
2. JSONDataOperationsTest2.java --> Loading json files using Java APIs
3. XMLDataOperationsTest3.java --> Loading xml files using Java APIs
4. BinaryLoadTest2.java --> Loading binary files (images) using Java APIs
5. POJOOperationsTest5.java <br>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;doJacksonOperationsTest --> Loading json documents as POJO writes using Jackson <br>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;doJAXBOperationsTest --> Loading xml documents as POJO writes using Jackson
6. SpringDataOperationsTest4.java --> Loading data with Spring Data Extension for MarkLogic 
7. SemanticsDataOperationsTest3.java --> Load and read Graph data (TTL format files are loaded). Executes SPARQL query on triples. 
8. SemanticsDataOperationsTest4.java 
		doGraphDataOperations --> Create a graph of triples, ontology, query with inferencing using RDFS and OWL HORST, OWL HORST FULL rulesets. Also demonstrates OWL sameAs relationship.  
9. JSONDMSDKOperationsTest4.java -->  Loads JSON documents using data movement SDK (DMSDK)
10. CSVDMSDKOperationsTest5.java --> Loads a CSV document, convert to JSON document (one row is one document) and loads to Marklogic using Data movement SDK (DMSDK)
11. JSONPatchDataOperationsTest5.java --> Load JSON files and patch with a POJO (uses insertFragment, PatchBuilder)
12. QueryOptionsOperationsTest5.java --> Test with different Query options.
13. PartialExtractOperationsTest5.java --> Extract portions of a document using query options including installation of query options.
14. POJORepositoryOperationsTest4.java --> Insert a POJO as a JSON using POJORepository. 
15. CustomPOJORepositoryOperationsTest4.java --> Insert a POJO as a JSON using a custom implementation of POJORepository. Also demonstrates adding metadata and properties to the document added using POJORepository. The validation is done by querying by metadata value using options.
16. RowManagerOperationsTest5.java --> Optic API usage demonstration. Few JSON documents are added, path range indexes are added. Then a relational view is created using Optic APIs. 
17. AlertingOperationsTest3.java --> Creates a rule and matches a transient document against that rule. (Alerting capability illustration)


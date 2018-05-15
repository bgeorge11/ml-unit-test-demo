package com.marklogic.test.suite1;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.URL;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.junit.Test;
import org.reflections.Reflections;
import org.reflections.scanners.MethodAnnotationsScanner;
import org.reflections.util.ClasspathHelper;
import org.reflections.util.ConfigurationBuilder;

public class GeneralUtils {

	/*
	 * This method matches the number associated with the test class name and
	 * returns the DB name associated with it Ex. DocumentLoadTest0 and
	 * unit-test- returns unit-test-0 DocumentLoadTest133 and unit-test- returns
	 * unit-test-133 DocumentLoad3Test200 and unit-test- returns unit-test-200
	 */

	public String getDBName(String className, String db_prefix)

	{
		Pattern p = Pattern.compile("(\\d+)\\D*$");
		Matcher m = p.matcher(className);
		if (m.find()) {
			return db_prefix + m.group();
		} else
			return "ERROR";

	}

	public int countTestCases(String patterns, String packageName) {
		int countOfCases = 0;
		int maxDbIndex = 0;
		int currentDbIndex = 0;
		int j=0;
		String[] strPatterns = patterns.split("\\s*,\\s*");
		String className = "";
		Reflections reflections = new Reflections(new ConfigurationBuilder()
				.setUrls(ClasspathHelper.forPackage(packageName)).setScanners(new MethodAnnotationsScanner()));
		Set<Method> methods = reflections.getMethodsAnnotatedWith(Test.class);
		Iterator<Method> it = methods.iterator();
		while (it.hasNext()) {
			className = it.next().getDeclaringClass().getSimpleName();
			for (int i = 0; i <= strPatterns.length - 1; i++) {
				if (className.contains(strPatterns[i])) {
					countOfCases++;
					currentDbIndex = Integer.parseInt(className.replaceAll("[^0-9]", ""));
					if (currentDbIndex > maxDbIndex) {
						maxDbIndex = currentDbIndex;
					}
				}
			}
			/*
			 * TODO If there could be more cases in one class, then do the
			 * distinct count
			 */
		}

		return maxDbIndex;

	}

	private static List<Class> findClasses(File directory, String packageName) throws ClassNotFoundException {
		List<Class> classes = new ArrayList<Class>();
		if (!directory.exists()) {
			return classes;
		}
		File[] files = directory.listFiles();
		for (File file : files) {
			if (file.isDirectory()) {
				assert !file.getName().contains(".");
				classes.addAll(findClasses(file, packageName + "." + file.getName()));
			} else if (file.getName().endsWith(".class")) {
				classes.add(
						Class.forName(packageName + '.' + file.getName().substring(0, file.getName().length() - 6)));
				System.out.println("CLASS NAME = " + file.getName());
			}
		}
		return classes;
	}

	public Class[] getClasses(String packageName, String pattern) throws ClassNotFoundException, IOException {
		ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
		assert classLoader != null;
		String path = packageName.replace('.', '/');
		Enumeration<URL> resources = classLoader.getResources(path);
		List<File> dirs = new ArrayList<File>();
		while (resources.hasMoreElements()) {
			URL resource = resources.nextElement();
			dirs.add(new File(resource.getFile()));
		}
		ArrayList<Class> classes = new ArrayList<Class>();
		for (File directory : dirs) {
			classes.addAll(findClasses(directory, packageName));
		}
		return classes.toArray(new Class[classes.size()]);
	}

	public String readContentsOfFile(String fileName) throws IOException {
		BufferedReader br = new BufferedReader(new FileReader(fileName));
		try {
			StringBuilder sb = new StringBuilder();
			String line = br.readLine();

			while (line != null) {
				sb.append(line);
				sb.append("\n");
				line = br.readLine();
			}
			return sb.toString();
		} finally {
			br.close();
		}
	}

	public void logComments(String log, String level) {
		if (level.equals("DEBUG")) {
			System.out.println(log);
		}
	}

	public static ArrayList<File> listFilesForFolder(final File folder, final boolean recursivity,
			final String patternFileFilter) {

		// Inputs
		boolean filteredFile = false;

		// Ouput
		final ArrayList<File> output = new ArrayList<File>();

		// Foreach elements
		for (final File fileEntry : folder.listFiles()) {

			// If this element is a directory, do it recursivly
			if (fileEntry.isDirectory()) {
				if (recursivity) {
					output.addAll(listFilesForFolder(fileEntry, recursivity, patternFileFilter));
				}
			} else {
				// If there is no pattern, the file is correct
				if (patternFileFilter.length() == 0) {
					filteredFile = true;
				}
				// Otherwise we need to filter by pattern
				else {
					filteredFile = Pattern.matches(patternFileFilter, fileEntry.getName());
				}

				// If the file has a name which match with the pattern, then add
				// it to the list
				if (filteredFile) {
					output.add(fileEntry);
				}
			}
		}

		return output;
	}
}

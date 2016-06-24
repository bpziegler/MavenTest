MavenTest
=========

Contains multiple maven projects in a single project (for quick & small projects)

To build the jar file, run these maven tasks "clean compile assembly:single"

To run the jar file:

java -jar target/MavenTest-0.0.1-SNAPSHOT-jar-with-dependencies.jar GraphCompactor [src_graph.db]


If you get a missing artifact for jdk tools jar:
http://stackoverflow.com/questions/11118070/buiding-hadoop-with-eclipse-maven-missing-artifact-jdk-toolsjdk-toolsjar1
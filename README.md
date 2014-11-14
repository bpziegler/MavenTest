MavenTest
=========

Contains multiple maven projects in a single project (for quick & small projects)

To build the jar file, run these maven tasks "clean compile assembly:single"

To run the jav file:

java -jar target/MavenTest-0.0.1-SNAPSHOT-jar-with-dependencies.jar GraphCompactor [src_graph.db]

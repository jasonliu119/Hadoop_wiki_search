HADOOP_HOME=/Users/WeijieLiu/Downloads/hadoop-1.0.4


make:
	javac Parser/MP4Parser.java

	javac -cp $(HADOOP_HOME)/lib/commons-cli-1.2.jar:$(HADOOP_HOME)/hadoop-core-1.0.4.jar *.java 
	jar cvf InvertedIndex.jar *.class

	javac Search/MP4SearchEngine.java

clean:
	rm InvertedIndex.jar
	rm *.class
	rm Parser/*.class
	rm Search/*.class

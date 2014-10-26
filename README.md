Hadoop_wiki_search
==================

a word search engine based on Hadoop


(1) modify the hadoop_home in the "makefile" 

(2) use command "make" to get the executables and you can get InvertedIndex.jar as the Hadoop jar 

(3) modify the hadoop path and name in the "run_test.sh"

(4) in this folder, I provide "test.xml" as the very small sample. You can modify the first line of "run_test.sh" :java Parser.MP4Parser test.xml 1 

	the first argument means the input XML file,  e.g. test.xml; the second argument means the number of the output parsed files

(5) the output parsed files is named as "00", "01", "02", "03" .... 

(6) You can put them into a folder, e.g. "input", and run the hadoop job 

(7) The last command is to run the Search Engine and it will asks you whether to use a default path of 20.xml and 1.xml. You should type in any other strings. 
	
	Then you should input the file path of the Inverted Index file (the output of Hadoop Job) and the folder path where the input files of Hadoop are 

	Then you can type in some words and ends your input with "!" to get the results. 

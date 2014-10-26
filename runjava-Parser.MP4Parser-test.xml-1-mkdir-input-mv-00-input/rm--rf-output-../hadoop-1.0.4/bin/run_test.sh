java Parser.MP4Parser test.xml 1

mkdir input
mv 00 input/
rm -rf output
../hadoop-1.0.4/bin/hadoop jar InvertedIndex.jar MP4InvertedIndex input output

java Search.MP4SearchEngine

all: PDPreProcess.class
	jar cf pre.jar PDPreProcess*.class PDNodeWritable*.class

compile:
	hadoop com.sun.tools.javac.Main PDPreProcess.java PDNodeWritable.java

clean:
	rm -f *.jar *.class

update_input:
	hadoop fs -put -f input/  /

run:
	rm -rf ./output ; \
	hadoop fs -rm -r -f /user/hadoop/output && \
	hadoop jar pre.jar PDPreProcess /user/hadoop/customeTestcase /user/hadoop/output 1 && \
	hadoop fs -get /user/hadoop/output ./ && \
	cat ./output/part*

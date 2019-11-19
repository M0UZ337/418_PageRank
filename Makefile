all: PageRank.class
	jar cf pr.jar PRPreProcess*.class PRNodeWritable*.class PageRank*.class

compile:
	hadoop com.sun.tools.javac.Main PRPreProcess.java PRNodeWritable.java PageRank.java

clean:
	rm -f *.jar *.class

update_input:
	hadoop fs -put -f input/  /

run:
	rm -rf ./output ; \
	hadoop fs -rm -r -f /user/hadoop/tmp && \
	hadoop fs -mkdir /user/hadoop/tmp && \
	hadoop fs -rm -r -f /user/hadoop/output && \
	hadoop jar pr.jar PageRank /user/hadoop/customeTestcase /user/hadoop/output && \
	hadoop fs -get /user/hadoop/output ./ && \
	cat ./output/part*

final:
	make compile && \
	make all && \
	make run

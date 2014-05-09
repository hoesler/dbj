PACKAGE_ROOT=..

.PHONY: all clean

all: inst

compile_java:
	find info/ -name "*.java" | xargs javac -source 1.4 -target 1.4

create_jar: compile_java
	find info/ -name "*.class" | xargs jar cvf RJDBC.jar

inst: create_jar
	mkdir -p $(PACKAGE_ROOT)/inst/java
	cp *.jar $(PACKAGE_ROOT)/inst/java

clean: clean_src clean_inst

clean_src:
	find . -name "*.class" -delete
	rm *.jar

clean_inst:
	rm -rf $(PACKAGE_ROOT)/inst/java
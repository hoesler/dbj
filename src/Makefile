PACKAGE_ROOT=..

.PHONY: all clean

all: inst

create_jar:
	mvn -f java/pom.xml package

inst: create_jar
	mkdir -p $(PACKAGE_ROOT)/inst/java
	cp java/target/*.jar $(PACKAGE_ROOT)/inst/java

clean: clean_src clean_inst

clean_src:
	mvn -f java/pom.xml clean

clean_inst:
	rm -rf $(PACKAGE_ROOT)/inst/java
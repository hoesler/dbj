PACKAGE_ROOT=..

.PHONY: all clean

all: inst

create_jar:
	mkdir -p java/target
	find java/target -name *.jar -delete
	mvn -f java/pom.xml package

inst: create_jar
	mkdir -p $(PACKAGE_ROOT)/inst/java
	find -E java/target -regex '.*dbj-[0-9]+\.[0-9]+\.jar' -exec cp {} $(PACKAGE_ROOT)/inst/java \;

clean: clean_target clean_inst

clean_target:
	mvn -f java/pom.xml clean

clean_inst:
	rm -rf $(PACKAGE_ROOT)/inst/java
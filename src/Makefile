PACKAGE_ROOT=..

.PHONY: all clean

all: inst

create_jar:
	mkdir -p java/target
	find java/target -name *.jar -delete
	env mvn -f java/pom.xml package

inst: create_jar
	mkdir -p $(PACKAGE_ROOT)/inst/java
	find java/target -name '*.jar' -not -name '*proguard*' -exec cp {} $(PACKAGE_ROOT)/inst/java \;

clean: clean_target clean_inst

clean_target:
	env mvn -f java/pom.xml clean

clean_inst:
	rm -rf $(PACKAGE_ROOT)/inst/java
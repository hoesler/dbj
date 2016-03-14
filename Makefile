PACKAGE_ROOT := .
JAVA_ROOT := $(PACKAGE_ROOT)/java
JAVA_BUILD_ROOT := $(JAVA_ROOT)/target
INST_PATH := $(PACKAGE_ROOT)/inst
INST_JAVA_PATH := $(INST_PATH)/java

.PHONY: all clean

all: inst

create_jar:
	mkdir -p $(JAVA_BUILD_ROOT)
	find $(JAVA_BUILD_ROOT) -name *.jar -delete
	env mvn -f $(JAVA_ROOT)/pom.xml package

inst: create_jar
	mkdir -p $(INST_JAVA_PATH)
	find $(JAVA_BUILD_ROOT) -name '*.jar' -not -name '*proguard*' -exec cp {} $(INST_JAVA_PATH) \;

clean: clean_target clean_inst

clean_target:
	env mvn -f $(JAVA_BUILD_ROOT)/pom.xml clean

clean_inst:
	rm -rf $(INST_JAVA_PATH)

JAR := $(shell find $(JAVA_BUILD_ROOT) -name '*.jar' -not -name '*proguard*' | head -1)
TEMPDIR_SOURCE := $(shell mktemp -d /tmp/dbj.XXXX)
TEMPDIR_INST := $(shell mktemp -d /tmp/dbj.XXXX)
JAR_BASENAME := $(shell basename $(JAR))
MD5_SOURCE = $(shell cd $(TEMPDIR_SOURCE) && exec find . -type f -not -name MANIFEST.MF -exec md5 {} \; | sort -k 34 | md5)
MD5_INST = $(shell cd $(TEMPDIR_INST) && exec find . -type f -not -name MANIFEST.MF -exec md5 {} \; | sort -k 34 | md5)
	
verify_inst: create_jar
	unzip "$(JAVA_BUILD_ROOT)/$(JAR_BASENAME)" -d "$(TEMPDIR_SOURCE)"
	unzip "$(INST_JAVA_PATH)/$(JAR_BASENAME)" -d "$(TEMPDIR_INST)"
	test $(MD5_SOURCE) = $(MD5_INST)
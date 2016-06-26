#!/bin/bash

mvn compile
mvn exec:java -Dexec.mainClass="com.daoyu.app.Test2Schema"
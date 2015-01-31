#!/bin/bash

javac -cp `hadoop classpath` DiseaseApplication_format_4.java -d diseaseApp_classes

jar -cvf diseaseapp.jar -C diseaseApp_classes/ .

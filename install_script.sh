#!/usr/bin/env bash

cd "$HOME/flink" || exit

sudo mvn -T 1C install -Dcheckstyle.skip -Drat.skip=true -DskipTests -Dfast -Dmaven.javadoc.skip=true -pl flink-streaming-java -nsu #-o

cd "$HOME/flink/flink-dist" || exit

sudo mvn -T 1C install -Dcheckstyle.skip -Drat.skip=true -DskipTests -Dfast -Dmaven.javadoc.skip=true -N

cd "$HOME/flink" || exit

sudo mvn -T 1C install -Dcheckstyle.skip -Drat.skip=true -DskipTests -Dfast -Dmaven.javadoc.skip=true -N

cp -r build-target/* ~/flink-binary

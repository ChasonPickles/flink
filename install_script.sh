#!/usr/bin/env bash

cd "$HOME/flink" || exit

sudo mvn -T 1C install -Dcheckstyle.skip -Drat.skip=true -DskipTests -Dfast -Dmaven.javadoc.skip=true -pl flink-streaming-java -nsu #-o

cd "$HOME/flink/flink-dist" || exit

sudo mvn -T 1C install -Dcheckstyle.skip -Drat.skip=true -DskipTests -Dfast -Dmaven.javadoc.skip=true -N

cd "$HOME/flink" || exit

sudo mvn -T 1C install -Dcheckstyle.skip -Drat.skip=true -DskipTests -Dfast -Dmaven.javadoc.skip=true -N

echo "Copying build-target to ~/flink-binary"
# cp -r build-target/* ~/flink-binary
rsync -av build-target/* ~/flink-binary --exclude=conf/flink-conf.yaml


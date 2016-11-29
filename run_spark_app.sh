#!/bin/bash
spark-submit --class $1 --master spark://sandbox.hortonworks.com:7077 ./target/develop-1.0-SNAPSHOT.jar

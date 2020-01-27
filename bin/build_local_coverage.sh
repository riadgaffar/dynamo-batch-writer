#!/bin/ksh

coverage erase
coverage run --branch --source=. --omit="**tests/**" -m pytest

if [ $? == 0 ]; then
    coverage xml -i
    sonar-scanner -X -Dsonar.python.coverage.reportPath=coverage.xml \
    -Dsonar.projectKey=dynamo-batch-writer \
    -Dsonar.exclusions=tests/**,coverage.xml,*.json,docs/**
    echo "http://localhost:9000/component_measures?id=dynamo-batch-writer&metric=coverage"
fi

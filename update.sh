#!/bin/bash

docker build  -t flame_test:latest .
docker tag flame_test:latest dev-harbor.personalhealthtrain.de/flame_test/flame-test:latest
docker push dev-harbor.personalhealthtrain.de/flame_test/flame-test:latest

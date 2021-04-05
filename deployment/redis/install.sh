#!/bin/bash

mkdir redis-env/confs -p
cp -r deployment/redis/confs redis-env/confs
cd redis-env

wget https://download.redis.io/releases/redis-6.2.1.tar.gz
tar xzf redis-6.2.1.tar.gz
cd redis-6.2.1
make

pip install redis
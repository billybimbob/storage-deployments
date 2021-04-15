#!/bin/bash

sudo add-apt-repository ppa:redislabs/redis -y
sudo apt-get update
sudo apt-get install redis

pip3 install redis

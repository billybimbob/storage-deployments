#!/bin/bash

sudo add-apt-repository ppa:redislabs/redis
sudo apt-get update
sudo apt-get install redis

pip3 install redis
#!/bin/sh

#Reinstall Java 7 because Maven probels
sudo apt-get remove openjdk-7-*
sudo apt-get install openjdk-7-jdk

#Set JAVA_HOME
export JAVA_HOME=/usr/lib/jvm/java-1.7.0-openjdk-amd64
export PATH=$PATH:$JAVA_HOME/bin

#Install Git
sudo apt-get install git
apt-get update

#Install maven
sudo apt-get install maven

#Clone the repo
git clone https://github.com/miguelangcosta/ads-manager.git
cd  ads-manager

# Package the project
mvn clean package




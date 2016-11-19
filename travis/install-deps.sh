#!/bin/bash

# MAC OS X
if [[ $TRAVIS_OS_NAME == 'osx' ]]; then
	brew update
	brew outdated ant || brew upgrade ant
	brew outdated libtool || brew upgrade libtool
	brew outdated libsqlite3-dev || brew upgrade libsqlite3-dev
	brew outdated bison || brew upgrade bison
	brew outdated flex || brew upgrade flex
	brew outdated || brew upgrade 
# LINUX	
else
	sudo apt-get -qq update
	sudo apt-get install -y ant autotools-dev autoconf bison flex git libsqlite3-dev libtool libreadline6 libreadline6-dev sqlite3 gnuplot
fi

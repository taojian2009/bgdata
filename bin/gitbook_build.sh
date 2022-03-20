#!/bin/bash

workdir=$1

cd $workdir

git pull

gitbook build



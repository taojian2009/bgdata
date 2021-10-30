#!/bin/bash


while [ true ]; do

git add .
git commit -m "update code"

git push
echo "updated at $date"

sleep 120s
done

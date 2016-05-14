#!/bin/bash

if ! [ "$(id 'fuller')" ]; then
  getent group 'fuller' > /dev/null 2>&1
  exit_code=$?
  if [ $exit_code -eq 0 ]; then
    echo "creating user fuller and adding to fuller group"
    useradd --no-create-home --system -g"fuller" "fuller"
  elif [ $exit_code -eq 2 ]; then
    echo "creating user and group fuller"
    useradd --no-create-home --system --user-group "fuller"
  else
    echo "could not get group info, failed"
    exit 1
  fi
fi

echo "creating log directory: /var/log/fullerite"
mkdir -p /var/log/fullerite
chown fuller:fuller /var/log/fullerite

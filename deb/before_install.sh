#!/bin/bash

if ! [ "$(id 'fuller')" ]; then
  if [ "$(grep -q "\bfuller\b" /etc/group)" ]; then
    echo "creating user fuller and adding to fuller group"
    useradd --no-create-home --system -g "fuller"
  else
    echo "creating user and group fuller"
    useradd --no-create-home --system --user-group "fuller"
  fi
fi

echo "creating log directory: /var/log/fullerite"
mkdir -p /var/log/fullerite
chown fuller:fuller /var/log/fullerite

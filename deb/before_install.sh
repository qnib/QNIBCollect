#!/bin/bash

if ! [ "$(id 'fuller')" ]; then
  echo "creating user fuller"
  useradd --no-create-home --system --user-group "fuller"
fi

echo "creating log directory: /var/log/fullerite"
mkdir -p /var/log/fullerite
chown fullerite:fullerite /var/log/fullerite

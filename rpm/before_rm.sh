#!/bin/bash

service 'fullerite' stop
service 'fullerite_diamond_server' stop

if [ "$(id 'fuller')" ]; then
  userdel 'fuller'
fi

rm -rf /var/log/fullerite

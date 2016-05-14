#!/bin/bash

set -e

case "$1" in
    purge)
        if [ "$(id 'fuller')" ]; then
            userdel 'fuller'
        fi
        rm -rf /var/log/fullerite
        ;;
    remove|upgrade|failed-upgrade|abort-install|abort-upgrade|disappear)
            ;;
    *)
        echo "postrm called with unknown argument \`$1'" >&2
        exit 1
        ;;
esac

exit 0

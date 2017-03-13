./src/corvus -n 10.254.127.37:6379,10.254.127.48:6379,10.254.127.49:6379 -t 4 -b 6379 -L debug ./corvus.conf  1>/var/log/corvus.log 2>/var/log/corvus.err &
echo $! > /var/run/corvus.pid

killall -9 corvus

/usr/bin/corvus -n 10.254.127.37:6379,10.254.127.48:6379,10.254.127.49:6379 -t 4 -b 6379 -L debug /etc/corvus.conf 1>/var/log/corvus.log 2>/var/log/corvus.err &
CORVUSPID=$!
echo Startted Corvus with PID=$CORVUSPID
echo $CORVUSPID > /var/run/corvus.pid
ps aux |grep corvus


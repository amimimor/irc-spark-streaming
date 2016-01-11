#!/bin/bash

# Here you might do some configs
nickname="testBot"
server="irc.wikimedia.org"
ircPort=6667
channels=(\#en.wikipedia \#fr.wikipedia \#de.wikipedia \#ja.wikipedia) # You might add multiple channels to join like this:
# (channel1 channel2 channel3), however ensures that you escape each #
logFile=log.log

# do connection and joining stuff
echo "Connecting to server ..."
exec 3<>/dev/tcp/$server/$ircPort || echo "Connection failure! ..."
echo -ne "NICK ${nickname}\r\n" >&3
echo -ne "USER ${nickname} localhost localhost :simple irc bash bot\r\n" >&3
echo "Connect to all channels ..."
for ind in ${!channels[*]}
do
  echo -ne "JOIN ${channels[$ind]}\r\n" >&3
done

# program main loop
while true
do
  # read the next line from the connection
  line=$(head -n 1 <&3)
  # check, if (stream) connection closed (network error or something like that)
  # Note, that IRC Server never send empty lines
  if [ "$line" == "" ] || [ "$(ls /proc/$$/fd | grep -w '3')" != "3" ]
  then
    echo "Connection to irc server was closed! Quitting ..."
    break
  fi
  # output on screen (Note, that the \n was removed by the head command)
  echo "Reciving ..."
  printf "%s\n" "$line"
  # reply PONG to PING else the IRC Server assumes that you're not listening
  # anymore and will kick you
  if [ "${line:0:5}" == "PING " ]
  then
    echo -n "PONG ${line:5}" >&3
    echo -ne "\r\n" >&3
  fi
  # Here more interaction stuff can be applied
  echo $line >> $logFile
done

exec 3>&- # close output connection
exec 3<&- # close input connection

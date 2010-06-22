
Eventually this will be turned into a some POD docs and the different topics that
this pertains to.

== Multiple MySQL instances on the same machine ==

http://blog.philipp-michels.de/?p=41

mysqlmanager --defaults-file=/etc/mysql/my.cnf

== Using balance ==

http://www.inlab.de/balance.html

Executing it:

  balance -f 61613 mq1.example.com:61613 mq2.example.com:61613

== Using Linux Virtual Server ==

http://www.linuxvirtualserver.org/software/ipvs.html

Executing it:

  ipvsadm -A -t external.example.com:61613 -s wlc
  ipvsadm -a -t external.example.com:61613 -r mq1.example.com:61613 -m
  ipvsadm -a -t external.example.com:61613 -r mq2.example.com:61613 -m




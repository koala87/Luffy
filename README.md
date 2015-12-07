# Luffy

Description:
KTV Info Server

Author:
Yingqi Jin <jinyingqi@luoha.com>

Modules:
generic:
    utility.py: generic routines
    business.py: template

route: forward app/box/erp/init requests to business modules

control: initial boxes and open/close room ...

client.py: app/box/erp

route.ini: config business server and forward rules

run.sh: wrapper to run route.py and control.py


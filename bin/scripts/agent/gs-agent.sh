#!/bin/sh
##########################################################################
#
# Copyright (C) 2010 EnterpriseDB Corporation.
# Copyright (C) 2011 Stado Global Development Group.
# Copyright (c) 2016 Regents of the University of Minnesota
#
# This file is part of the Minnesota Population Center's Terra Populus project.
# For copyright and licensing information, see the NOTICE and LICENSE files
# in this project's top-level directory, and also online at:
# https://github.com/mnpopcenter/stado
#
# gs-agent.sh
#
#
# Starts a Stado agent process
#
##########################################################################


EXECCLASS=org.postgresql.stado.util.XdbAgent

GSCONFIG=../config/stado_agent.config

# Adjust these if more memory is required
MINMEMORY=256M
MAXMEMORY=256M

echo "Starting...."

nohup java -classpath ../lib/stado.jar:../lib/log4j.jar:../lib/postgresql.jar:${CLASSPATH} -Xms${MINMEMORY} -Xmx${MAXMEMORY} -Dconfig.file.path=${GSCONFIG} $EXECCLASS $* > ../log/agent.log 2>&1 &

PROCID=$!

sleep 3

ps $PROCID >/dev/null 2>/dev/null
CHECK=$?

if [ "$CHECK" -ne "0" ]
then
	echo "Error starting XDBAgent"
	echo " agent.log output:"
	cat ../log/agent.log
	echo ""
	echo " tail of console.log output:"
	tail -10 ../log/console.log
fi


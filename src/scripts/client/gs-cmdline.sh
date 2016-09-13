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
# gs-cmdline.sh
#
#
# Used for getting a SQL command prompt
#
##########################################################################

EXECCLASS=org.postgresql.stado.util.CmdLine

java -classpath ../lib/stado.jar:../lib/jline-0_9_5.jar:../lib/log4j.jar:../lib/postgresql.jar:${CLASSPATH} $EXECCLASS $* 

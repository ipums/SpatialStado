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
# gs-shutdown.sh
#
#
# Shuts down the Stado server
#
##########################################################################


EXECCLASS=org.postgresql.stado.util.XdbShutdown

GSCONFIG=../config/stado.config

java -classpath ../lib/stado.jar:../lib/log4j.jar:../lib/postgresql.jar -Dconfig.file.path=${GSCONFIG} $EXECCLASS "$@"


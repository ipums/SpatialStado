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
# gs-impex.sh
#
#
# Used for importing and exporting with Stado. 
# If populating with a large amount of data, use XDBLoader instead.
#
##########################################################################

EXECCLASS=org.postgresql.stado.util.XdbImpEx

java -classpath ../lib/stado.jar:../lib/log4j.jar:../lib/postgresql.jar $EXECCLASS "$@"


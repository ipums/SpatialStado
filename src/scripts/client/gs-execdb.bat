@echo off
rem ###########################################################################
rem Copyright (C) 2010 EnterpriseDB Corporation.
rem Copyright (C) 2011 Stado Global Development Group.
rem Copyright (c) 2016 Regents of the University of Minnesota
rem
rem This file is part of the Minnesota Population Center's Terra Populus project.
rem For copyright and licensing information, see the NOTICE and LICENSE files
rem in this project's top-level directory, and also online at:
rem https://github.com/mnpopcenter/stado
rem 
rem
rem gs-execdb.bat
rem
rem
rem Executes a command against all underlying database instances
rem
rem ###########################################################################

set EXECCLASS=org.postgresql.stado.util.ExecDb

set GSCONFIG=..\config\stado.config

java -classpath ..\lib\stado.jar;..\lib\log4j.jar -Dconfig.file.path=%GSCONFIG% %EXECCLASS% %*%




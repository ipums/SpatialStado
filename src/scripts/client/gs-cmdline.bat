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
rem gs-cmdline.bat
rem
rem
rem Used for getting a SQL command prompt
rem
rem ###########################################################################

set EXECCLASS=org.postgresql.stado.util.CmdLine

java -classpath ..\lib\stado.jar;..\lib\jline-0.9.5.jar;..\lib\log4j.jar %EXECCLASS% %*%

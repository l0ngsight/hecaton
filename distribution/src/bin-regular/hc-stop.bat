@echo off

REM ******* This script kills java processes running HecatonMemberStarter

wmic process where "Name like '%%java%%' AND CommandLine like '%%HecatonMemberStarter%%'" delete

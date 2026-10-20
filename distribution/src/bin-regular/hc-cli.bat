@echo off

SETLOCAL ENABLEDELAYEDEXPANSION

CALL "%~dp0common.bat"

if %errorlevel% neq 0 (
    exit /b %errorlevel%
)

echo "%RUN_JAVA%" %displayOpts% -cp %CLASSPATH% uk.org.hecaton.client.console.HecatonCommandLine %*
"%RUN_JAVA%" %filteredOpts% -cp %CLASSPATH% uk.org.hecaton.client.console.HecatonCommandLine %*

ENDLOCAL

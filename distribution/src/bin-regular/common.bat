
:: detect Java

if "x%JAVA_HOME%" == "x" (
    echo JAVA_HOME is not set, running Java as 'java'
    set RUN_JAVA=java
) else (
    echo JAVA_HOME is set, running Java at '%JAVA_HOME%\bin\java'
    set "RUN_JAVA=%JAVA_HOME%\bin\java"
)

"%RUN_JAVA%" -version 1>nul 2>nul || (
    echo JAVA could not be found in your system.
    echo Please install Java and ensure either JAVA_HOME
    echo is defined, or the java executable is in the PATH.
    exit /b 2
)

:: %~dp0 is the drive+path where this script lives
if "x%HECATON_HOME%" == "x" (
    echo HECATON_HOME is not set, expecting Hecaton at '%~dp0..'
    set HECATON_HOME=%~dp0..
) else (
    echo HECATON_HOME is set, expecting Hecaton at '%HECATON_HOME%'
)

:: options

:: ******* you can enable following variables by uncommenting them

:: ******* minimum heap size
:: set MIN_HEAP_SIZE=1G

:: ******* maximum heap size
:: set MAX_HEAP_SIZE=1G


if NOT "%MIN_HEAP_SIZE%" == "" (
	set JAVA_OPTS=%JAVA_OPTS% -Xms%MIN_HEAP_SIZE%
)

if NOT "%MAX_HEAP_SIZE%" == "" (
	set JAVA_OPTS=%JAVA_OPTS% -Xmx%MAX_HEAP_SIZE%
)

:: options

FOR /F "tokens=* USEBACKQ" %%F IN (`CALL "%RUN_JAVA%" -cp "%HECATON_HOME%\lib\*" uk.org.hecaton.internal.util.JavaVersion`) DO SET JAVA_VERSION=%%F

IF %JAVA_VERSION% GEQ 9 (
    SET JAVA_OPTS=%JAVA_OPTS%%=%--add-modules java.se^
 --add-exports java.base/jdk.internal.ref=ALL-UNNAMED^
 --add-opens java.base/java.lang=ALL-UNNAMED^
 --add-opens java.base/sun.nio.ch=ALL-UNNAMED^
 --add-opens java.management/sun.management=ALL-UNNAMED^
 --add-opens jdk.management/com.sun.management.internal=ALL-UNNAMED

    FOR /F "tokens=2 delims== USEBACKQ" %%A IN (`CALL "%RUN_JAVA%" -XshowSettings:properties -version 2^>^&1 ^| findstr /c:"java.vm.name"`) DO SET VM_NAME=%%A

    :: trim leading ws, trailing ws
    FOR /F "tokens=* delims= " %%A IN ("!VM_NAME!") DO SET VM_NAME=%%~A
    FOR /L %%a in (1,1,15) DO IF "!VM_NAME:~-1!"==" " SET VM_NAME=!VM_NAME:~0,-1!

    IF NOT "x!VM_NAME:OpenJ9=!" == "x!VM_NAME!" (
    	REM OpenJ9 detected, adding additional exports
    	set JAVA_OPTS=%JAVA_OPTS% --add-exports jdk.management/com.ibm.lang.management.internal=ALL-UNNAMED
    )
)
REM Disable the log4j2 shutdown hook, otherwise log lines during node shutdown might be lost
set JAVA_OPTS=%JAVA_OPTS% -Dlog4j.shutdownHookEnabled=false -Dhecaton.logging.shutdown=true

:: HECATON_CONFIG holds path to the configuration file. The path is relative to the Hecaton installation (HECATON_HOME).
if "x%HECATON_CONFIG%" == "x" (
    set HECATON_CONFIG=config\hecaton.xml
    if not exist "%HECATON_HOME%\!HECATON_CONFIG!" (
      set HECATON_CONFIG=config\hecaton.yaml
    )
    if not exist "%HECATON_HOME%\!HECATON_CONFIG!" (
      set HECATON_CONFIG=config\hecaton.yml
    )
    if not exist "%HECATON_HOME%\!HECATON_CONFIG!" (
      echo "Configuration file is missing. Create hecaton.[xml|yaml|yml] in %HECATON_HOME%\config or set the HECATON_CONFIG environment variable."
      exit /b 2
    )
    echo HECATON_CONFIG is not set, using '%HECATON_HOME%\!HECATON_CONFIG!'
) else (
    echo HECATON_CONFIG is set, using configuration file at '!HECATON_CONFIG!'
)

set JAVA_OPTS=%JAVA_OPTS% -Dhecaton.config="%HECATON_HOME%\!HECATON_CONFIG!"
:: Suppress Java 24 and 25 warnings for deprecated Unsafe methods.
:: This is required because Hecaton and JCTools internally use Unsafe API.
:: Java 23: 'allow' is the default, so no explicit setting is needed.
:: Java 24-25: 'warn' is the default, so we explicitly set it to 'allow'.
:: Java 26 or later: 'deny' will be the default, and 'allow' option will no longer be supported.
IF %JAVA_VERSION% GEQ 24 IF %JAVA_VERSION% LEQ 25 (
    set JAVA_OPTS=%JAVA_OPTS% --sun-misc-unsafe-memory-access=allow
)
:: Java 24 and later warn when a library calls native code (JEP 472) unless it is enabled.
IF %JAVA_VERSION% GEQ 22 (
    set JAVA_OPTS=%JAVA_OPTS% --enable-native-access=ALL-UNNAMED
)
:: classpath

set CLASSPATH="%HECATON_HOME%\lib\*;%HECATON_HOME%\bin\user-lib;%HECATON_HOME%\bin\user-lib\*;%CLASSPATH%"

REM Mask sensitive options in JAVA_OPTS
set "maskKeys="
set "filteredOpts="
set "displayOpts="

set "LINE_TO_PROCESS=%JAVA_OPTS%"
:parse_loop
for /f "tokens=1,* delims= " %%A in ("!LINE_TO_PROCESS!") do (
    set "opt=%%A"
    set "opt=!opt:"=!"
    set "LINE_TO_PROCESS=%%B"

    if "!opt!"=="!opt:-DmaskOpts=!" (
        set "filteredOpts=!filteredOpts! !opt!"
    ) else (
        for /f "tokens=1* delims==" %%A in ("!opt!") do (
            set "maskKeys=%%B"
        )
    )
)
if defined LINE_TO_PROCESS goto parse_loop

set "LINE_TO_PROCESS=!filteredOpts!"
:filter_loop
for /f "tokens=1,* delims= " %%A in ("!LINE_TO_PROCESS!") do (
    set "opt=%%A"
    set "opt=!opt:"=!"
    set "LINE_TO_PROCESS=%%B"

    set "masked="
    for %%K in (!maskKeys!) do (
        echo !opt! | findstr /c:"-D%%K=" >nul
        if !errorlevel! == 0 (
            set "displayOpts=!displayOpts! -D%%K=****"
            set "masked=true"
        )
    )

    if not defined masked (
        set "displayOpts=!displayOpts! !opt!"
    )
)
if defined LINE_TO_PROCESS goto filter_loop


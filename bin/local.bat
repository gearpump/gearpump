@echo off
 
echo "Example: %0 -port <port> -sameprocess -workernum <4>"

SET port=0
SET sameprocess="false"
SET workernum=1

:loop
IF NOT "%1"=="" (
    IF "%1"=="-port" (
        SET port=%2
        SHIFT
    )
	IF "%1"=="-workernum" (
        SET workernum=%2
        SHIFT
    )
    IF "%1"=="-sameprocess" (
        SET sameprocess="false"        
    )
    SHIFT
    GOTO :loop
)

echo java -cp "lib/*" org.apache.gears.cluster.Starter local -port %port% -sameprocess %sameprocess% -workernum %workernum%
java -cp "lib/*" org.apache.gears.cluster.Starter local -port %port% -sameprocess %sameprocess% -workernum %workernum%
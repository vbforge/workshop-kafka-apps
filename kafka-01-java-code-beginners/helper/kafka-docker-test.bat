@echo off
echo ========================================
echo Kafka Docker Test Script
echo ========================================
echo.

:menu
echo Choose an option:
echo 1) List topics
echo 2) Create topic (demo_topic_example)
echo 3) Describe topic
echo 4) Start console consumer
echo 5) Start console producer
echo 6) List consumer groups
echo 7) Describe consumer group
echo 8) Delete topic
echo 9) Reset consumer offsets
echo 10) View Kafka logs
echo q) Quit
echo.

set /p choice="Enter choice: "

if "%choice%"=="1" goto list
if "%choice%"=="2" goto create
if "%choice%"=="3" goto describe
if "%choice%"=="4" goto consume
if "%choice%"=="5" goto produce
if "%choice%"=="6" goto groups
if "%choice%"=="7" goto describeGroup
if "%choice%"=="8" goto delete
if "%choice%"=="9" goto resetOffsets
if "%choice%"=="10" goto logs
if "%choice%"=="q" exit
goto menu

:list
docker exec -it kafka-java-broker kafka-topics --bootstrap-server localhost:9092 --list
pause
goto menu

:create
docker exec -it kafka-java-broker kafka-topics --bootstrap-server localhost:9092 --create --topic demo_topic_example --partitions 3 --replication-factor 1
pause
goto menu

:describe
docker exec -it kafka-java-broker kafka-topics --bootstrap-server localhost:9092 --describe --topic demo_topic_example
pause
goto menu

:consume
echo Starting consumer... Press Ctrl+C to stop
docker exec -it kafka-java-broker kafka-console-consumer --bootstrap-server localhost:9092 --topic demo_topic_example --from-beginning --property print.key=true --property print.partition=true
pause
goto menu

:produce
echo Starting producer... Type messages and press Enter. Ctrl+C to stop.
docker exec -it kafka-java-broker kafka-console-producer --bootstrap-server localhost:9092 --topic demo_topic_example --property parse.key=true --property key.separator=:
pause
goto menu

:groups
docker exec -it kafka-java-broker kafka-consumer-groups --bootstrap-server localhost:9092 --list
pause
goto menu

:describeGroup
set /p group="Enter group ID (default: my-java-application): "
if "%group%"=="" set group=my-java-application
docker exec -it kafka-java-broker kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group %group%
pause
goto menu

:delete
docker exec -it kafka-java-broker kafka-topics --bootstrap-server localhost:9092 --delete --topic demo_topic_example
pause
goto menu

:resetOffsets
set /p group="Enter group ID (default: my-java-application): "
if "%group%"=="" set group=my-java-application
docker exec -it kafka-java-broker kafka-consumer-groups --bootstrap-server localhost:9092 --group %group% --reset-offsets --to-earliest --topic demo_topic_example --execute
pause
goto menu

:logs
docker-compose logs -f kafka
goto menu
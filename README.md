setup:

# 1. Create directory and download Hadoop 3.3.6 Windows binaries
New-Item -ItemType Directory -Path "C:\hadoop\bin" -Force | Out-Null
Invoke-WebRequest -Uri "https://raw.githubusercontent.com/cdarlint/winutils/master/hadoop-3.3.6/bin/winutils.exe" -OutFile "C:\hadoop\bin\winutils.exe"
Invoke-WebRequest -Uri "https://raw.githubusercontent.com/cdarlint/winutils/master/hadoop-3.3.6/bin/hadoop.dll" -OutFile "C:\hadoop\bin\hadoop.dll"

# 2. Persist environment variables for current and future sessions
[Environment]::SetEnvironmentVariable("HADOOP_HOME", "C:\hadoop", "User")
[Environment]::SetEnvironmentVariable("Path", $env:Path + ";C:\hadoop\bin", "User")
$env:HADOOP_HOME = "C:\hadoop"
$env:Path += ";C:\hadoop\bin"

# 3. Create fresh checkpoints directory
New-Item -ItemType Directory -Path "C:\hadoop\checkpoints\spark_console" -Force | Out-Null

# 4. Activate virtual environment and install project dependencies
Set-Location "C:\Users\admin\Documents\project\pract\streaming"
& "C:\Users\admin\Documents\project\pract\.venv\Scripts\Activate.ps1"
pip install confluent-kafka pyspark faker



====================

Start Kafka & Create Topic

Set-Location "C:\Users\admin\Documents\project\pract\streaming\docker"
docker compose up -d
Start-Sleep -Seconds 5
docker exec -it local-kafka kafka-topics --create --topic retail-transactions --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
docker exec -it local-kafka kafka-topics --list --bootstrap-server localhost:9092

====================

start the producer

Set-Location "C:\Users\admin\Documents\project\pract\streaming\producer"
& "C:\Users\admin\Documents\project\pract\.venv\Scripts\Activate.ps1"
python stream_producer.py

==============================

run the spark streaming consumer

Set-Location "C:\Users\admin\Documents\project\pract\streaming\consumer_streaming"
& "C:\Users\admin\Documents\project\pract\.venv\Scripts\Activate.ps1"
python spark_streaming.py
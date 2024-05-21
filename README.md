# spark_standalone
Create a spark standalone app using docker

First, clone the git repo!

Steps to execute the main.py which answers 3 questions asked

**Steps:**

Use your terminal 

 create container -
-     docker build -t standalone-spark3:3.0.2 .
 Run the container -
-     docker-compose up
 In an another terminal
 to check if spark master and worker is up and running -
-     docker ps
grab the container id of master
-     docker exec -it <CONTAINER ID>  bash
  d. Go to /opt/spark/bi
-     execute spark submit command - ./spark-submit --master spark://spark-master:7077 --driver-memory 1G --executor-memory 1G /opt/spark-apps/main.py

To stop spark master and single/only container: 
-     docker stop $(docker ps -a -q)




Screenshots:

Q1) <img width="678" alt="image" src="https://github.com/09Sayali/spark_standalone/assets/61360725/578e29f1-53b8-486d-b57d-ededdeee686a">

Q2) <img width="671" alt="image" src="https://github.com/09Sayali/spark_standalone/assets/61360725/a85dae0e-fe52-4f56-b6e9-b64af8e77174">

Q3) <img width="510" alt="image" src="https://github.com/09Sayali/spark_standalone/assets/61360725/a6b4b6d0-db55-4b95-85e5-8e3011049e06">
    just few lines, sorting by year not asked






Run instructions:

    <1> docker-compose build

    <2> docker-compose up --scale spark-worker=3 -d

    <3> docker exec spark-master spark-submit --master spark://spark-master:7077 apps/spark.py

Access Spark Web UI:

    - http://localhost:9090/


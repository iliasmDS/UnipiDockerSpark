
[Reproducability Steps (Docker)]:

 <1> Install docker:

    - WINDOWS: https://docs.docker.com/desktop/install/windows-install/

    - LINUX: https://docs.docker.com/desktop/install/linux/
    
 <2> Install docker desktop (optional):

    - WINDOWS: https://docs.docker.com/desktop/install/windows-install/

    - LINUX: https://docs.docker.com/desktop/install/linux/

 <3> Use the (docker desktop) terminal to navigate to the DockerSpark directory

[Run instructions]:

    <1> docker-compose build

   The will build the docker-compose image.

    <2> docker-compose up --scale spark-worker=4 -d

   Uses the image to make the master container and the specified number of worker containers.

    <3> docker exec spark-master spark-submit --master spark://spark-master:7077 apps/SPARK_ASJ.py 4 5

    <3-alternative> -//- with apps/SPARK_ANN.py

    <3-alternative> -//- with apps/TEST.py

   This command makes the spark-submit as the master node.
   Submitting the "SPARK.py" file.
   The 4 (first argument) changes the executors RAM, while the 5 (second argument) changes their cores.

[Access Spark Web UI]:

    - http://localhost:9090/

[Running additionals (python code)]:

    <1> pip install -r just_python\requirements.txt

    <2> docker exec spark-master spark-submit --master spark://spark-master:7077 apps/TEST.py

    <3> Run All: BRUTE_FORCE.ipynb

    <4> Run All: READ_RESULTS.ipynb
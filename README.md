# Introduction

For retargeting companies that provide online display advertisements like Criteo, it is important to identify what are the valid clicks. In the context of this project, we will seek to detect some of those suspicious/fraudulent activities by creating a Flink application which will read from Kafka clicks and displays. 

From these two topics, we will extract the fraudulent patterns that have been simulated and store the results in an output file.
We can distinguish this project in two parts: offline analysis and online implementation. For simplicity, it was recommended to do offline analysis which indeed proved to be more efficient.

Then in a second step, we implemented a Flink application on Scala in order to manage online data and detect fraudulent behavior in real time.


# Project environment and execution

We wanted at first to explore the data on Scala but not being familiar with this language and as recommended to detect outliers, we finally chose to do an offline analysis before. 

To do so, we collected the data streams in real time by directly reading and writing them into a text file. We then investigated in Python language via a Google Colab notebook to make it easier for both of us to access after each change. To run our offline analysis, one can execute the notebook
[offline\_analysis.ipynb](https://colab.research.google.com/drive/1QW6CSgzzblAAuJAfa8ljjy4Aosp7HbaQ?usp=sharing) or visualise it through this [Github repository](https://github.com/emiliechhean/Suspicious_activities/blob/main/offline_analysis.ipynb).

The offline data used can be found in [medium repository](https://github.com/emiliechhean/Suspicious_activities/blob/main/medium).

After doing the offline analysis, we can now turn to the online part :

First, one has to run the docker-compose file  __*docker-compose.yml*__ by executing __*docker-compose rm -f; docker-compose up*__ in the same directory as the file. 

Then, one can execute [StreamingJob.scala](https://github.com/emiliechhean/Suspicious_activities/blob/main/streaming/quickstart/src/main/scala/org/myorg/quickstart/StreamingJob.scala) to launch our fraudulent activities detector to retrieve the patterns and its outputs. Two output files will be generated *suspiciousUID.txt* and *suspiciousIP.txt* - the meaning of these files are explained in the following sections. An example of them is given in [suspiciousInputs repository](https://github.com/emiliechhean/Suspicious_activities/tree/main/streaming/quickstart/suspiciousInputs).

It has been done in Scala and on IntelliJ IDE, that can be found in this Github via the repository [quickstart](https://github.com/emiliechhean/Suspicious_activities/tree/main/streaming/quickstart).

# Introduction

For retargeting companies that provide online display advertisements like Criteo, it is important to identify what are the valid clicks. In the context of this project, we will seek to detect some of those suspicious/fraudulent activities by creating a Flink application which will read from Kafka clicks and displays. 

From these two topics, we will extract the fraudulent patterns that have been simulated and store the results in an output file.
We can distinguish this project in two parts: offline analysis and online implementation. For simplicity, it was recommended to do offline analysis which indeed proved to be more efficient.

Then in a second step, we implemented a Flink application on Scala in order to manage online data and detect fraudulent behavior in real time.


# Project environment and execution

We wanted at first to explore the data on Scala but not being familiar with this language and as recommended to detect outliers, we finally chose to do an offline analysis before.

To do so, we collected the data streams in real time by directly reading and writing them into a text file. We decided to investigate in Python language via a Google Colab to make it easier for both of us to access after each change. To run our offline analysis, one has to execute the notebook [offline_analysis.ipynb].

After doing the offline analysis, we can now turn to the online part. First, one has to run the docker-compose filedocker-compose.yml by executing ‘docker-compose rm-f; docker-compose up‘ in the same directory as the file. After doing this, one can execute StreamingJob.scala to launch our fraudulent activities detector to retrieve the patterns and its outputs.

Two output files will be generated: suspiciousUID.txt and suspiciousIP.txt - the meaning of these files are explained in the following sections.

Finally, to sum up we have one part with offline analysis in Python language in a Google Colab notebook and in this current repository via the file offline_analysis.ipynb. We have also provided in the same folder the dataset used for this exploration work.

The second part concerns the online analysis done on Scala and in the IntelliJ IDE, that can be found in this Github via the repository quickstart.
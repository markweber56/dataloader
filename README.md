### How to build the package
1. sbt clean package
2. mkdir jars
3. cp target/scala-2.12/dataloader_2.12-0.1.0-SNAPSHOT.jar jars/

### How to run the Example
To run the Scala code for this chapter use:

* `spark-submit --class main.scala.dataloader.DataLoader jars/dataloader_2.12-0.1.0-SNAPSHOT.jar `

### Run from IntelliJ
Use Java 1.8

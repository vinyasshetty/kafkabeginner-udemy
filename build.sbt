name := "kafkabeginnerudemy"

version := "0.1"

scalaVersion := "2.12.3"

organization := "com.viny"

libraryDependencies ++= Seq("org.apache.kafka" % "kafka-clients" % "2.3.0" ,
                            "org.slf4j" % "slf4j-simple" % "1.7.29",
                            "com.twitter" %  "hbc-core" % "2.2.0",
                            "org.elasticsearch.client" % "elasticsearch-rest-high-level-client" % "7.2.0",
                            "com.google.code.gson" % "gson" % "2.8.6",
                            "org.apache.kafka" % "kafka-streams" % "2.3.0"
                           )
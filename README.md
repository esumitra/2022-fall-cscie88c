## Programming in Scala for Big Data Systems, Spring 2022
Scala Project for Harvard Extension course CSCI-E88C, Spring, 2022. See https://courses.dce.harvard.edu/?details&srcdb=202202&crn=26278 for more details.


The project requires Java 8 or Java 11, Scala 2.13 and sbt 1.5.2+ environment to run.

### Getting started
 Use the following commands to get started with your project

 - Compile: `sbt compile`
 - Create a "fat" jar: `sbt assembly`
 - Run tests: `sbt test`
 - To install in local repo: `sbt publishLocal`

### Static Analysis Tools

#### Scalafmt
To ensure clean code, run scalafmt periodically. The scalafmt configuration is defined at https://scalameta.org/scalafmt/docs/configuration.html

For source files,

`sbt scalafmt`

For test files.

`sbt test:scalafmt`

#### Scalafix
To ensure clean code, run scalafix periodically. The scalafix rules are listed at https://scalacenter.github.io/scalafix/docs/rules/overview.html

For source files,

`sbt "scalafix RemoveUnused"`

For test files.

`sbt "test:scalafix RemoveUnused"`

### License
Copyright 2022, Edward Sumitra

Licensed under the MIT License.
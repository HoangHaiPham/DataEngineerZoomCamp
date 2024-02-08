## MacOS

Here we'll show you how to install Spark 3.5.0 for MacOS.

### Installing Java

Ensure Brew and Java installed in your system:

```bash
xcode-select --install
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install.sh)"
brew install openjdk@11
```

Add the following environment variables to your `.bash_profile` or `.zshrc`:

```bash
export JAVA_HOME="/usr/local/Cellar/openjdk@11/11.0.22"
export PATH="${JAVA_HOME}/bin/:${PATH}"
```

Make sure Java was installed to `/usr/local/Cellar/openjdk@11/11.0.22`: Open Finder > Press Cmd+Shift+G > paste "/usr/local/Cellar/openjdk@11/11.0.22". If you can't find it, then change the path location to appropriate path on your machine. You can also run `brew info java` to check where java was installed on your machine.

### Installing Spark

1. Install Scala

```bash
brew install scala
```

2. Install Apache Spark

```bash
brew install apache-spark
```

3. Add environment variables:

Add the following environment variables to your `.bash_profile` or `.zshrc`. Replace the path to `SPARK_HOME` to the path on your own host. Run `brew info apache-spark` to get this.

```bash
export SPARK_HOME="/usr/local/Cellar/apache-spark/3.5.0/libexec"
export PATH="${SPARK_HOME}/bin/:${PATH}"
```

### Testing Spark

Execute `spark-shell` and run the following in scala:

```scala
val data = 1 to 10000
val distData = sc.parallelize(data)
distData.filter(_ < 10).collect()
```

### PySpark

It's the same for all platforms. Go to [pyspark.md](pyspark.md).

**Note**: In order to check the path, use command

> which java

> which pyspark

> echo $SPARK_HOME

> echo $PATH

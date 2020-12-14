import com.typesafe.config.{Config, ConfigFactory}

class AppConfig(config: Config) {

  // validate vs. reference.conf
  config.checkValid(ConfigFactory.defaultReference())

  def this() {
    this(ConfigFactory.load())
  }

  val kmerLength        = config.getInt("spark-sample-app.kmer-length")
}

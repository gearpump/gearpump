package org.apache.gearpump.experiments.yarn

import com.typesafe.config.Config
import org.apache.gearpump.cluster.main.ParseResult
import org.scalatest.FlatSpecLike
import org.specs2.mock.Mockito
import org.specs2.mutable.Specification


class AppConfigTest extends FlatSpecLike with Mockito {

  "The AppConfig" should "return value from config when cliopts are null and getEnv is called" in {
    val config = mock[Config]
    val appConfig = new AppConfig(null, config)
    val key = "key"
    appConfig.getEnv(key)
    one(config).getString(key)
  }

  it should "return value from config when cliopts doesn't have corresponding key and getEnv is called" in {
    val config = mock[Config]
    val cliopts = mock[ParseResult]
    val appConfig = new AppConfig(cliopts, config)
    val key = "key"
    cliopts.exists(key) returns(false)
    appConfig.getEnv(key)
    one(config).getString(key)
  }

  it should "return value from cliopts when cliopts has corresponding key and getEnv is called" in {
    val config = mock[Config]
    val cliopts = mock[ParseResult]
    val appConfig = new AppConfig(cliopts, config)
    val key = "key"
    cliopts.exists(key) returns(true)
    appConfig.getEnv(key)
    one(cliopts).getString(key)
  }
}

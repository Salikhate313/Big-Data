package com.ecommerce

import com.ecommerce.utils.ConfigLoader
import com.typesafe.config.ConfigFactory
import org.scalatest.funsuite.AnyFunSuite

class ConfigLoaderSpec extends AnyFunSuite {

  test("valeurs par défaut quand les clés sont absentes") {
    val cfg = ConfigLoader.fromConfig(ConfigFactory.empty())
    assert(cfg.minAge == 16 && cfg.maxAge == 100)
    assert(cfg.shufflePartitions == 8)
    assert(cfg.transactionsPath == "data/transactions.csv")
    assert(cfg.enableCache && cfg.enableBroadcast)
  }

  test("les valeurs du fichier priment sur les défauts") {
    val cfg = ConfigLoader.fromConfig(ConfigFactory.parseString(
      "app.validation.user.min-age = 18\napp.optimization.enable-cache = false"))
    assert(cfg.minAge == 18)
    assert(!cfg.enableCache)
  }
}

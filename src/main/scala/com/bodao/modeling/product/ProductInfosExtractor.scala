package com.bodao.modeling.product

import org.json4s.{DefaultFormats, JValue}
import org.json4s.jackson.JsonMethods._

//{"pid":"5555", "name":"面膜", "price": 12.0, "sales": 1000}
case class ProductInfos(pid: String, name: String, price: String, sales: String)

object ProductInfosExtractor {
  implicit val formats = DefaultFormats

  @inline
  private def extractPid(js: JValue) = {
    (js \\ "pid").extractOpt[String]
  }

  @inline
  private def extractName(js: JValue) = {
    (js \\ "name").extractOpt[String]
  }

  @inline
  private def extractPrice(js: JValue) = {
    (js \\ "price").extractOpt[String]
  }

  @inline
  private def exctractSales(js: JValue) = {
    (js \\ "sales").extractOpt[String]
  }

  @inline
  private def parsingProductInfos(productInfosStr: String): Option[JValue] = {
    try {
      Some(parse(productInfosStr))
    } catch {
      case ex: Exception => None
    }
  }

  def extractProductInfos(productInfosStr: String): Option[ProductInfos] = {
    this.parsingProductInfos(productInfosStr) match {
      case None => None
      case Some(productInfosJs) =>
        for {
          pid <- this.extractPid(productInfosJs)
          name <- this.extractName(productInfosJs)
          price <- this.extractPrice(productInfosJs)
          sales <- this.exctractSales(productInfosJs)
        } yield ProductInfos(pid, name, price, sales)
    }
  }
}

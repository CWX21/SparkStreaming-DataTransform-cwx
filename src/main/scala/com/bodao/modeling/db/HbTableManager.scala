package com.bodao.modeling.db

import top.spoofer.hbrdd._
import top.spoofer.hbrdd.config.HbRddConfig
import top.spoofer.hbrdd.hbsupport.{FamilyPropertiesStringSetter, HbRddFamily}

object HbTableManager {
  private val tableName = "KafkaToHbaseProduct"

  private def getPropertiesFamilys = {
    /* 修改列簇的maxversions为100 */
    val cf1 = HbRddFamily("ProductInfos", FamilyPropertiesStringSetter(Map("maxversions" -> "100")))
    Set(cf1)
  }

  def createTable(force: Boolean = false)(implicit hbRddConfig: HbRddConfig) = {
    val admin = HbRddAdmin.apply()

    if (force) {  //强制建表前需要先删除原来的表
      admin.dropTable(tableName)
    }

    admin.createTableByProperties(tableName, this.getPropertiesFamilys)
    admin.close()
  }
}

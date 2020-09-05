package com.yaoting117.project.titan.dataware.beans

case class AppLogBean(
    var guid: Long,
    eventid: String,
    event: Map[String, String],
    uid: String,
    imei: String,
    mac: String,
    imsi: String,
    osName: String,
    osVer: String,
    androidId: String,
    resolution: String,
    deviceType: String,
    deviceId: String,
    uuid: String,
    appid: String,
    appVer: String,
    release_ch: String,
    promotion_ch: String,
    longitude: Double,
    latitude: Double,
    carrier: String,
    netType: String,
    cid_sn: String,
    ip: String,
    sessionId: String,
    timestamp: Long,
    var province: String = "未知",
    var city: String = "未知",
    var district: String = "未知"
)

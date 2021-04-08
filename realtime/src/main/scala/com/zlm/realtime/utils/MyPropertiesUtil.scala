package com.zlm.realtime.utils
import java.io.InputStreamReader
import java.util.Properties

/**
 * @author Harbour 
 * @date 2021-04-02 10:16
 */
object MyPropertiesUtil {

    def main(args: Array[String]): Unit = {
        val prop: Properties = MyPropertiesUtil.load("config.properties")
        println(prop.getProperty("kafka.broker.list"))
    }

    def load(path: String): Properties = {
        val prop: Properties = new Properties()
        prop.load(new InputStreamReader(
            Thread.currentThread().getContextClassLoader.getResourceAsStream(path), "UTF-8"))
        prop
    }

}

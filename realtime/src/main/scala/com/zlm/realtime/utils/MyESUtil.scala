package com.zlm.realtime.utils

import com.zlm.realtime.bean.DauInfo
import io.searchbox.client.config.HttpClientConfig
import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.core.{Bulk, BulkResult, DocumentResult, Get, Index, Search, SearchResult}
import org.elasticsearch.index.query.{BoolQueryBuilder, MatchQueryBuilder, QueryBuilder, TermQueryBuilder}
import org.elasticsearch.search.builder.SearchSourceBuilder
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder
import org.elasticsearch.search.sort.SortOrder

import java.util

/**
 * @author Harbour 
 * @date 2021-03-31 21:49
 */
object MyESUtil {

    private var factory : JestClientFactory = null

    def getClient : JestClient = {
        if (factory == null) build()
        factory.getObject
    }

    def build (): Unit = {
        factory = new JestClientFactory
        factory.setHttpClientConfig(
            new HttpClientConfig.Builder("http://hadoop102:9200")
              .multiThreaded(true)
              .maxTotalConnection(20)
              .connTimeout(10000)
              .readTimeout(1000)
              .build()
        )
    }

    def putIndex(): Unit = {
        // 获取连接，关闭连接
        val client: JestClient = getClient

        // 创建一个资源
        val source : String = """{
                                |  "id": 300,
                                |  "name": "incident red sea",
                                |  "doubanScore": 5,
                                |  "actorList": [
                                |    {
                                |      "id": 4,
                                |      "name": "zhang san feng"
                                |    }
                                |  ]
                                |}""".stripMargin

        val index: Index = new Index.Builder(source)
          .index("movie_index_1")
          .`type`("movie")
          .id("1")
          .build()

        // 执行一条插入语句
        client.execute(index)

        client.close()
    }

    def getIndex(): Unit = {
        val client: JestClient = getClient
        val get : Get = new Get.Builder("movie_index_1", "1").build();
        val result: DocumentResult = client.execute(get)
        println(result.getJsonString)
        client.close()
    }

    def queryIndex(): Unit = {
        val client: JestClient = getClient

        val query: String = """{
                              |  "query": {
                              |    "bool": {
                              |      "must": [
                              |        {
                              |          "match": {
                              |            "name": "red"
                              |          }
                              |        }
                              |      ],
                              |      "filter": {
                              |        "term": {
                              |          "actorList.name.keyword": "zhang han yu"
                              |        }
                              |      }
                              |    }
                              |  },
                              |  "from": 0,
                              |  "size": 20,
                              |  "sort": [
                              |    {
                              |      "doubanScore": {
                              |        "order": "desc"
                              |      }
                              |    }
                              |  ],
                              |  "highlight": {
                              |    "fields": {
                              |      "name": {}
                              |    }
                              |  }
                              |}""".stripMargin

        val search: Search = new Search.Builder(query)
          .addIndex("movie_index")
          .build()
        val result: SearchResult = client.execute(search)
        val list: util.List[SearchResult#Hit[util.Map[String, Any], Void]] =
            result.getHits(classOf[util.Map[String, Any]])

        //将 Java 转换为 Scala 集合，方便操作
        import scala.collection.JavaConverters._
        val list1: List[util.Map[String, Any]] = list.asScala.map(_.source).toList
        println(list1.mkString("\n"))

        client.close()
    }

    def queryIndex1(): Unit = {
        val client: JestClient = getClient

        val searchSourceBuilder = new SearchSourceBuilder

        val boolQueryBuilder: BoolQueryBuilder = new BoolQueryBuilder()
        boolQueryBuilder.must(new MatchQueryBuilder("name", "red"))
        boolQueryBuilder.filter(new TermQueryBuilder("actorList.name.keyword","zhang han yu"))
        searchSourceBuilder
          .from(0)
          .size(20)
          .sort("doubanScore", SortOrder.DESC)
          .highlighter(new HighlightBuilder().field("name"))
        searchSourceBuilder.query(boolQueryBuilder)

        val query: String = searchSourceBuilder.toString()

        val search: Search = new Search.Builder(query)
          .addIndex("movie_index")
          .build()
        val result: SearchResult = client.execute(search)
        val list: util.List[SearchResult#Hit[util.Map[String, Any], Void]] =
            result.getHits(classOf[util.Map[String, Any]])

        //将 Java 转换为 Scala 集合，方便操作
        import scala.collection.JavaConverters._
        val list1: List[util.Map[String, Any]] = list.asScala.map(_.source).toList
        println(list1.mkString("\n"))

        client.close()
    }

    def bulkInsert(dauList: List[DauInfo], indexName: String): Unit = {
        val client: JestClient = getClient
        val bulkBuilder = new Bulk.Builder
        for (dau <- dauList) {
            val index: Index = new Index.Builder(dau)
              .index(indexName)
              .`type`("_doc")
              .build()
            bulkBuilder.addAction(index)
        }
        val bulk: Bulk = bulkBuilder.build()
        val result: BulkResult = client.execute(bulk)
        val items: util.List[BulkResult#BulkResultItem] = result.getItems
        println("保存到 ES " + items.size() + " 条数据")
        //关闭连接
        client.close()
    }

    def main(args: Array[String]): Unit = {
        queryIndex()
    }

}

package com.zlm.mall.publisher.service.impl;

import com.zlm.mall.publisher.service.ESService;
import io.searchbox.client.JestClient;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.TermsAggregation;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Harbour
 * @date 2021-04-08 9:19
 */

@Service
public class ESServiceImpl implements ESService {

    @Resource
    JestClient jestClient;

    @Override
    public Long getDauTotal(String date) {

        SearchSourceBuilder query = new SearchSourceBuilder().query(new MatchAllQueryBuilder());
        String indexName = "mall2021_dau_info_" + date + "-query";

        Search search = new Search.Builder(query.toString())
                .addIndex(indexName)
                .addType("_doc")
                .build();

        long totalDau = 0L;

        try {
            SearchResult result = jestClient.execute(search);
            if (result.getTotal() != null) totalDau = result.getTotal();
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("ES查询失败");
        }
        return totalDau;
    }

    @Override
    public Long getNewUserTotal(String date) {
        return 10086L;
    }

    @Override
    public Map<String, Long> getDauHour(String date) {

        HashMap<String, Long> dauHourMap = new HashMap<>();

        SearchSourceBuilder builder = new SearchSourceBuilder()
                .aggregation(AggregationBuilders.terms("groupByHour").field("hr").size(24));

        String query = builder.toString();
        String indexName = "mall2021_dau_info_" + date + "-query";
        Search search = new Search.Builder(query)
                .addIndex(indexName)
                .addType("_doc")
                .build();
        try {
            SearchResult result = jestClient.execute(search);
            TermsAggregation group = result.getAggregations().getTermsAggregation("groupByHour");
            if (group != null) {
                for (TermsAggregation.Entry bucket : group.getBuckets()) {
                    String key = bucket.getKey();
                    Long value = bucket.getCount();
                    dauHourMap.put(key, value);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("ES查询失败");
        }
        return dauHourMap;
    }
}

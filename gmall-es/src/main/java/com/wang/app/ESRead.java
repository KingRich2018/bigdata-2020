package com.wang.app;

import com.alibaba.fastjson.JSONObject;
import com.wang.untils.ESClientUtil;
import io.searchbox.client.JestClient;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.max.MaxAggregationBuilder;
import org.elasticsearch.search.aggregations.support.ValueType;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class ESRead {
    public static void main(String[] args) throws IOException {
        JestClient jestClient = ESClientUtil.getJestClient("http://hadoop102:9200");

        // 创建查询语句的对象
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        // bool
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        boolQueryBuilder.filter(new TermQueryBuilder("sex","male"));
        boolQueryBuilder.must(new MatchQueryBuilder("favo","编程"));
        searchSourceBuilder.query(boolQueryBuilder);

        //aggs
        TermsAggregationBuilder termsAggregationBuilder = new TermsAggregationBuilder("count_by_class", ValueType.LONG);
        termsAggregationBuilder.field("age");
        searchSourceBuilder.aggregation(termsAggregationBuilder);

        MaxAggregationBuilder maxAggregationBuilder = new MaxAggregationBuilder("max_age");
        maxAggregationBuilder.field("age");
        searchSourceBuilder.aggregation(maxAggregationBuilder);

        // 分页
        searchSourceBuilder.from(0);
        searchSourceBuilder.size(2);

        Search search = new Search.Builder(searchSourceBuilder.toString()).build();

        // 执行查询
        SearchResult result = jestClient.execute(search);

        //解析查询
        System.out.println("获取数据"+result.getTotal()+"条");
        System.out.println("最高匹配值"+result.getMaxScore());


        // 获取数据
        List<SearchResult.Hit<Map, Void>> hits = result.getHits(Map.class);
        for (SearchResult.Hit<Map, Void> hit : hits) {
            JSONObject jsonObject = new JSONObject();
            Map source = hit.source;
            for (Object o : source.keySet()) {
                jsonObject.put((String) o,source.get(o));
            }
            jsonObject.put("index",hit.index);
            jsonObject.put("type",hit.type);
            jsonObject.put("id",hit.id);
            System.out.println(jsonObject.toString());
        }

        ESClientUtil.close(jestClient);
    }
}

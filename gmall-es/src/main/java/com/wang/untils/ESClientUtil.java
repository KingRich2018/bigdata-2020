package com.wang.untils;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;

/**
 * es 连接工具
 */
public class ESClientUtil {


    public static JestClient getJestClient(String url){
        //创建es客户端
        JestClientFactory jestClientFactory = new JestClientFactory();
        HttpClientConfig httpClientConfig = new HttpClientConfig.Builder(url).build();
        jestClientFactory.setHttpClientConfig(httpClientConfig);
        return jestClientFactory.getObject();
    }

    public static void close(JestClient jestClient){
        jestClient.shutdownClient();
    }
}

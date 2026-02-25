package com.experiment.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.web.client.RestTemplate;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import java.net.HttpURLConnection;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;

/**
 * RestTemplate 配置。提供标准 RestTemplate 和放宽 SSL 校验的 RestTemplate（用于 api.hyperliquid.xyz 等证书路径问题）。
 */
@Configuration
public class RestTemplateConfig {

    @Bean
    public RestTemplate restTemplate() {
        return new RestTemplate();
    }

    /** Kraken 全量 tickers 响应大(~150KB)，需更长超时 */
    @Bean(name = "longTimeoutRestTemplate")
    public RestTemplate longTimeoutRestTemplate() {
        SimpleClientHttpRequestFactory f = new SimpleClientHttpRequestFactory();
        f.setConnectTimeout(15000);
        f.setReadTimeout(30000);
        return new RestTemplate(f);
    }

    /**
     * 用于 Hyperliquid 等可能 PKIX 证书路径问题的 API。
     * 仅信任连接建立，不验证服务端证书（适用于开发/内网，生产环境建议配置正确 CA）。
     * 超时 10s/15s 防止请求挂起导致空数据。
     */
    @Bean(name = "sslRelaxedRestTemplate")
    public RestTemplate sslRelaxedRestTemplate() {
        SimpleClientHttpRequestFactory sslFactory = new SimpleClientHttpRequestFactory() {
            @Override
            protected void prepareConnection(HttpURLConnection connection, String httpMethod) throws java.io.IOException {
                super.prepareConnection(connection, httpMethod);
                try {
                    if (connection instanceof HttpsURLConnection https) {
                        SSLContext ssl = SSLContext.getInstance("TLS");
                        TrustManager[] trustAll = new TrustManager[]{new X509TrustManager() {
                            public void checkClientTrusted(X509Certificate[] chain, String authType) {}
                            public void checkServerTrusted(X509Certificate[] chain, String authType) {}
                            public X509Certificate[] getAcceptedIssuers() { return new X509Certificate[0]; }
                        }};
                        ssl.init(null, trustAll, new SecureRandom());
                        https.setSSLSocketFactory(ssl.getSocketFactory());
                        https.setHostnameVerifier((h, s) -> true);
                    }
                } catch (Exception e) {
                    throw new RuntimeException("SSL relaxed config failed", e);
                }
            }
        };
        sslFactory.setConnectTimeout(10000);
        sslFactory.setReadTimeout(15000);
        return new RestTemplate(sslFactory);
    }
}

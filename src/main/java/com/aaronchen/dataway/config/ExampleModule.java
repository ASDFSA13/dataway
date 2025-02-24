package com.aaronchen.dataway.config;

import com.alibaba.nacos.api.config.ConfigFactory;
import com.alibaba.nacos.api.config.ConfigService;
import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.client.config.impl.ClientWorker;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import net.hasor.core.ApiBinder;
import net.hasor.core.DimModule;
import net.hasor.dataql.QueryApiBinder;
import net.hasor.db.JdbcModule;
import net.hasor.db.Level;
import net.hasor.spring.SpringModule;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.stereotype.Component;
import net.hasor.dataql.Finder;

import javax.sql.DataSource;
import java.util.List;
import java.util.concurrent.Executor;

/**
 * @Author: Aaron chen
 * @Date: 2020/5/1 16:13
 */
@DimModule
@Component
public class ExampleModule implements SpringModule {
    @Autowired
    private DataSource dataSource = null;
    private DataSource dataSource1 = null;
    private DataSource dataSource2 = null;

    @Value("${nacos.config.server-addr:127.0.0.1:8848}")
    private String nacosServerAddr;  // Nacos 服务器地址

    @Value("${nacos.config.data-id:dataway_config}")
    private String dataId;  // 配置的 Data ID

    @Value("${nacos.config.group:dataway_config}")
    private String group;  // 配置的 Group

    @Value("${spring.datasource.username}")
    private String username;
    @Value("${nacos.config.namespace:local}")
    private String namespace;  // 配置的 Namespace，默认为 "local"
    private ConfigService configService;

    @Value("${spring.datasource.password}")
    private String password;
    @Override
    public void loadModule(ApiBinder apiBinder) throws Throwable {
        configService=createNacosConfigService(); // 创建 Nacos 配置服务`

        // 获取初始数据库配置
        List<String> databaseUrls = getDatabaseUrlsFromNacos();
        updateDataSource(databaseUrls);

        // 添加 Nacos 配置监听器，监听配置变化
        addNacosConfigListener();


        // .DataSource form Spring boot into Hasor 最基本的例程 来做一个初始化单数据源
        apiBinder.installModule(new JdbcModule(Level.Full, this.dataSource));

        // 参考官方工程源码注册dataSource 批量安装模块
//        QueryApiBinder queryBinder = apiBinder.tryCast(QueryApiBinder.class);
        // dataql中的命名"ds1" ； Level.Full 作用域的范围；this.dataSource1具体的数据源的指明
//        this.dataSource1
//        apiBinder.installModule(new JdbcModule(Level.Full, this.dataSource));

//        apiBinder.installModule(new JdbcModule(Level.Full, "ds1", this.dataSource1));
//        apiBinder.installModule(new JdbcModule(Level.Full, "ds2", this.dataSource2));
        // udf/udfSource/import 指令 的类型创建委托给 spring
//        queryBinder.bindFinder(apiBinder);


    }

    // 从 Nacos 加载数据库配置
    private List<String> getDatabaseUrlsFromNacos() throws NacosException {
//        ConfigService configService = createNacosConfigService();  // 创建 Nacos 配置服务
        String content = configService.getConfig(dataId, group, 5000);  // 获取配置内容
        return parseDatabaseUrls(content);  // 解析数据库 URL 配置
    }

    // 创建 Nacos 配置服务
//    private ConfigService createNacosConfigService() throws NacosException {
//        // 创建 Nacos 客户端
//        return new ClientWorker().getConfigService(nacosServerAddr);  // 获取 Nacos 配置服务
//    }
    // 获取 Nacos 配置类服务
    private ConfigService createNacosConfigService() throws NacosException {
        // 使用 ConfigFactory 创建 ConfigService 实例
        ConfigService configService=ConfigFactory.createConfigService(nacosServerAddr);
        return configService;
    }

    // 解析数据库 URL 配置
    private List<String> parseDatabaseUrls(String content) {
        // 使用 Jackson 或其他 JSON 序列化工具解析 JSON 内容
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            DatabaseConfig databaseConfig = objectMapper.readValue(content, DatabaseConfig.class);
            return databaseConfig.getDatabaseUrls();
        } catch (JsonProcessingException e) {
            throw new RuntimeException("解析 Nacos 配置失败", e);
        }
    }

    // 创建 DataSource 实例
    private DataSource createDataSource(String databaseUrl) {
        return DataSourceBuilder.create()
                .url(databaseUrl)
                .username(username)
                .password(password)
                .driverClassName("com.mysql.cj.jdbc.Driver")
                .build();
    }

    // 动态更新 DataSource
    private void updateDataSource(List<String> databaseUrls) {
        // init
        this.dataSource1 = createDataSource(databaseUrls.get(0));

        this.dataSource2 = createDataSource(databaseUrls.get(1))   ;
    }

    // 注册 Nacos 配置监听器
    private void addNacosConfigListener() throws NacosException {
        configService.addListener(dataId, group, new com.alibaba.nacos.api.config.listener.Listener() {
            @Override
            public Executor getExecutor() {
                // 返回 null 表示使用默认线程池执行
                return null;
            }

            @Override
            public void receiveConfigInfo(String configInfo) {
                // 配置变化时，重新加载配置并更新 DataSource
                List<String> databaseUrls = parseDatabaseUrls(configInfo);
                updateDataSource(databaseUrls);
                System.out.println("Nacos 配置已更新，DataSource 已更换。");
            }
        });
    }

//    private void addNacosConfigListener() throws NacosException {
//        configService.addListener(dataId, group, new com.alibaba.nacos.api.config.listener.Listener() {
//            @Override
//            public Executor getExecutor() {
//                return null;
//            }
//
//            @Override
//            public void receiveConfigInfo(String configInfo) {
//                // 配置变化时，重新加载配置并更新 DataSource
//                List<String> databaseUrls = parseDatabaseUrls(configInfo);
//                updateDataSource(databaseUrls);
//                System.out.println("Nacos 配置已更新，DataSource 已更换。");
//            }
//
//            public void lostConnection() {
//                System.out.println("Nacos 配置失去连接。");
//            }
//
//            public void connecetionRecovered() {
//                System.out.println("Nacos 配置连接恢复。");
//            }
//        });
//    }


    // 内部类，表示数据库配置
    public static class DatabaseConfig {
        private List<String> databaseUrls;

        public List<String> getDatabaseUrls() {
            return databaseUrls;
        }

        public void setDatabaseUrls(List<String> databaseUrls) {
            this.databaseUrls = databaseUrls;
        }
    }

}

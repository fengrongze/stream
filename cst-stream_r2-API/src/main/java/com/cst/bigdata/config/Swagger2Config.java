package com.cst.bigdata.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.ResourceHandlerRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurerAdapter;
import springfox.documentation.builders.ApiInfoBuilder;
import springfox.documentation.builders.PathSelectors;
import springfox.documentation.builders.RequestHandlerSelectors;
import springfox.documentation.service.ApiInfo;
import springfox.documentation.service.Contact;
import springfox.documentation.spi.DocumentationType;
import springfox.documentation.spring.web.plugins.Docket;
import springfox.documentation.swagger2.annotations.EnableSwagger2;

import static com.google.common.base.Predicates.or;
import static springfox.documentation.builders.PathSelectors.regex;

/**
 * @author Johnney.chiu
 * create on 2017/11/22 12:02
 * @Description 配置swagger2
 */
@Configuration
@EnableSwagger2
public class Swagger2Config extends WebMvcConfigurerAdapter {
    private final static String VERSION = "0.0.5";
    /*@Bean
    public Docket createRestApi() {
        return new Docket(DocumentationType.SWAGGER_2)
                .apiInfo(apiInfo())
                .select()
                .apis(RequestHandlerSelectors.basePackage("com.cst.bigdata.controller"))
                .paths(PathSelectors.any())
                .build();
    }
    private ApiInfo apiInfo() {
        return new ApiInfoBuilder()
                .title("Spring Boot中使用Swagger2构建RESTful APIs")
                .description("xuexiwenjian")
                .termsOfServiceUrl("http://localhost:8088/swagger-ui.html")
                .version("1.0.0")
                .build();
    }*/


    @Bean
    public Docket daySoureDocket() {
        Docket swaggerSpringMvcPlugin = new Docket(DocumentationType.SWAGGER_2)
                .groupName("DAY_SOURCE_API").apiInfo(daySourceApiInfo()).select()
                .paths(or(regex("/stream/day/source.*")))
                .build();

        return swaggerSpringMvcPlugin;
    }

    private ApiInfo daySourceApiInfo() {
        return new ApiInfoBuilder()
                .title("天第一条数据")
                .description("包含AM DE GPS OBD TRACE TRACEDELETE VOLTAGE等类型的天第一条数据")
                .version(VERSION)
                .contact(new Contact("cst", "http://www.kartor.cn", "qiuqiangqiang@kartor.cn"))
                .build();
    }


    @Bean
    public Docket hourSoureDocket() {
        Docket swaggerSpringMvcPlugin = new Docket(DocumentationType.SWAGGER_2)
                .groupName("HOUR_SOURCE_API").apiInfo(hourSourceApiInfo()).select()
                .paths(or(regex("/stream/hour/source.*")))
                .build();

        return swaggerSpringMvcPlugin;
    }

    private ApiInfo hourSourceApiInfo() {
        return new ApiInfoBuilder()
                .title("小时第一条数据（分类别）")
                .description("包含AM DE GPS OBD TRACE TRACEDELETE VOLTAGE等类型的小时第一条数据")
                .version(VERSION)
                .contact(new Contact("cst", "http://www.kartor.cn", "qiuqiangqiang@kartor.cn"))
                .build();
    }

    @Bean
    public Docket hourStatisticsDocket() {
        Docket swaggerSpringMvcPlugin = new Docket(DocumentationType.SWAGGER_2)
                .groupName("HOUR_STATISTIC_API").apiInfo(hourStatisticsApiInfo()).select()
                .paths(or(regex("/stream/hour/statistics.*")))
                .build();

        return swaggerSpringMvcPlugin;
    }

    private ApiInfo hourStatisticsApiInfo() {
        return new ApiInfoBuilder()
                .title("小时统计数据（分类别）")
                .description("包含AM DE GPS OBD TRACE TRACEDELETE VOLTAGE等类型的小时统计数据")
                .version(VERSION)
                .contact(new Contact("cst", "http://www.kartor.cn", "qiuqiangqiang@kartor.cn"))
                .build();
    }


    @Bean
    public Docket dayStatisticsDocket() {
        Docket swaggerSpringMvcPlugin = new Docket(DocumentationType.SWAGGER_2)
                .groupName("DAY_STATISTIC_API").apiInfo(dayStatisticsApiInfo()).select()
                .paths(or(regex("/stream/day/statistics.*")))
                .build();

        return swaggerSpringMvcPlugin;
    }

    private ApiInfo dayStatisticsApiInfo() {
        return new ApiInfoBuilder()
                .title("天统计数据（分类别）")
                .description("包含AM DE GPS OBD TRACE TRACEDELETE VOLTAGE等类型的天统计数据")
                .version(VERSION)
                .contact(new Contact("cst", "http://www.kartor.cn", "qiuqiangqiang@kartor.cn"))
                .build();
    }

    @Bean
    public Docket monthStatisticsDocket() {
        Docket swaggerSpringMvcPlugin = new Docket(DocumentationType.SWAGGER_2)
                .groupName("MONTH_STATISTIC_API").apiInfo(monthStatisticsApiInfo()).select()
                .paths(or(regex("/stream/month/statistics.*")))
                .build();

        return swaggerSpringMvcPlugin;
    }

    private ApiInfo monthStatisticsApiInfo() {
        return new ApiInfoBuilder()
                .title("月统计数据（分类别）")
                .description("包含AM DE GPS OBD TRACE TRACEDELETE VOLTAGE等类型的月统计数据")
                .version(VERSION)
                .contact(new Contact("cst", "http://www.kartor.cn", "qiuqiangqiang@kartor.cn"))
                .build();
    }

    @Bean
    public Docket yearStatisticsDocket() {
        Docket swaggerSpringMvcPlugin = new Docket(DocumentationType.SWAGGER_2)
                .groupName("YEAR_STATISTIC_API").apiInfo(yearStatisticsApiInfo()).select()
                .paths(or(regex("/stream/year/statistics.*")))
                .build();

        return swaggerSpringMvcPlugin;
    }

    private ApiInfo yearStatisticsApiInfo() {
        return new ApiInfoBuilder()
                .title("年统计数据（分类别）")
                .description("包含AM DE GPS OBD TRACE TRACEDELETE VOLTAGE等类型的年统计数据")
                .version(VERSION)
                .contact(new Contact("cst", "http://www.kartor.cn", "qiuqiangqiang@kartor.cn"))
                .build();
    }


    @Bean
    public Docket integratedStatisticsDocket() {
        Docket swaggerSpringMvcPlugin = new Docket(DocumentationType.SWAGGER_2)
                .groupName("INTEGRATED_STATISTIC_API").apiInfo(integratedStatisticsApiInfo()).select()
                .paths(or(regex("/stream/integrated/statistics/hour.*"),
                        regex("/stream/integrated/statistics/day.*"),
                        regex("/stream/integrated/statistics/year.*"),
                        regex("/stream/integrated/statistics/month.*")
                        ))
                .build();

        return swaggerSpringMvcPlugin;
    }

    private ApiInfo integratedStatisticsApiInfo() {
        return new ApiInfoBuilder()
                .title("整合的小时、天、月、年统计数据")
                .description("包含小时、天、月、年的单条数据查询和区间多条数据查询以及实时数据查询")
                .version(VERSION)
                .contact(new Contact("cst", "http://www.kartor.cn", "qiuqiangqiang@kartor.cn"))
                .build();
    }


    @Bean
    public Docket outAPIStatisticsDocket() {
        Docket swaggerSpringMvcPlugin = new Docket(DocumentationType.SWAGGER_2)
                .groupName("OUT_API").apiInfo(outAPIStatisticsApiInfo()).select()
                .paths(or(regex("/stream/integrated/statistics/hour.*"),
                        regex("/stream/integrated/statistics/day.*"),
                        regex("/stream/integrated/statistics/year.*"),
                        regex("/stream/integrated/statistics/month.*"),
                        regex("/stream/day/statistics/obd.*"),
                        regex("/oilPrice.*")
                ))
                .build();

        return swaggerSpringMvcPlugin;
    }

    private ApiInfo outAPIStatisticsApiInfo() {
        return new ApiInfoBuilder()
                .title("整合的小时、天、月、年统计数据")
                .description("包含小时、天、月、年的提供给外部的接口")
                .version(VERSION)
                .contact(new Contact("cst", "http://www.kartor.cn", "qiuqiangqiang@kartor.cn"))
                .build();
    }


    @Bean
    public Docket cityOilDocket() {
        Docket swaggerSpringMvcPlugin = new Docket(DocumentationType.SWAGGER_2)
                .groupName("CITY_OIL").apiInfo(cityOilApiInfo()).select()
                .paths(or(regex("/oilPrice.*")))
                .build();

        return swaggerSpringMvcPlugin;
    }

    private ApiInfo cityOilApiInfo() {
        return new ApiInfoBuilder()
                .title("油费查询")
                .description("包含城市油费查询等")
                .version(VERSION)
                .contact(new Contact("cst", "http://www.kartor.cn", "qiuqiangqiang@kartor.cn"))
                .build();
    }

    @Bean
    public Docket validateDocket() {
        Docket swaggerSpringMvcPlugin = new Docket(DocumentationType.SWAGGER_2)
                .groupName("VALIDATE").apiInfo(validateApiInfo()).select()
                .paths(or(regex("/validate.*")))
                .build();

        return swaggerSpringMvcPlugin;
    }

    private ApiInfo validateApiInfo() {
        return new ApiInfoBuilder()
                .title("数据验证")
                .description("数据验证，包含小时天数据验证")
                .version(VERSION)
                .contact(new Contact("cst", "http://www.kartor.cn", "qiuqiangqiang@kartor.cn"))
                .build();
    }


    @Override
    public void addResourceHandlers(ResourceHandlerRegistry registry) {
        registry.addResourceHandler("swagger-ui.html")
                .addResourceLocations("classpath:/META-INF/resources/");
        registry.addResourceHandler("/webjars/**")
                .addResourceLocations("classpath:/META-INF/resources/webjars/");
    }

}

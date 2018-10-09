package com.cst.bigdata.test.doc;


import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.cst.bigdata.BigdataHbaseApplication;
import io.github.robwin.swagger2markup.GroupBy;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.restdocs.AutoConfigureRestDocs;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.MockMvc;

import io.github.robwin.markup.builder.MarkupLanguage;
import io.github.robwin.swagger2markup.Swagger2MarkupConverter;
import springfox.documentation.staticdocs.SwaggerResultHandler;

import static org.springframework.restdocs.mockmvc.RestDocumentationRequestBuilders.get;


/**
 * @author Johnney.Chiu
 * create on 2018/7/9 15:07
 * @Description
 * @title
 */

@AutoConfigureMockMvc
@AutoConfigureRestDocs(outputDir = "target/generated-snippets")
@RunWith(SpringRunner.class)
@SpringBootTest(classes = BigdataHbaseApplication.class)//工程的启动类

public class SwaggerToHTMLandPDFTest {


    private String snippetDir = "target/generated-snippets";
    private String outputDir  = "target/asciidoc";

    @Autowired
    private MockMvc mockMvc;


    @Test
    public void Test() throws Exception {
        // 得到swagger.json,写入outputDir目录中
        mockMvc.perform(get("/v2/api-docs?group=OUT_API").accept(MediaType.APPLICATION_JSON))
                .andDo(SwaggerResultHandler.outputDirectory(outputDir).build())
                .andExpect(status().isOk())
                .andReturn();
        System.out.println(outputDir + "/swagger.json");
        // 读取上一步生成的swagger.json转成asciiDoc,写入到outputDir
        // 这个outputDir必须和插件里面<generated></generated>标签配置一致
        Swagger2MarkupConverter.from(outputDir + "/swagger.json")
                .withPathsGroupedBy(GroupBy.TAGS)// 按tag排序
                .withMarkupLanguage(MarkupLanguage.ASCIIDOC)// 格式
                .withExamples(snippetDir)

                .build()
                .intoFolder(outputDir);// 输出

        System.out.println("over");
    }

}

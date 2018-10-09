package com.cst.bigdata.test.controller;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.mock.web.MockServletContext;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.context.web.WebAppConfiguration;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * @author Johnney.chiu
 * create on 2018/1/8 15:09
 * @Description 单元测试
 */
@RunWith(SpringRunner.class)
@SpringBootTest(classes = MockServletContext.class)
@WebAppConfiguration
public class CstStreamHourIntegrateControllerTest {

    private static final String root_path = "/stream/hour";
    private MockMvc mvc;
    @Before
    public void setUp() throws Exception {
       // mvc = MockMvcBuilders.standaloneSetup(new CstStreamHourStatisticController()).build();
    }

    @Test
    public void getHello() throws Exception {
        System.out.println(mvc.perform(MockMvcRequestBuilders.get(root_path + "/am/find/M201411180010/1513755546000").accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk()));


    }


}

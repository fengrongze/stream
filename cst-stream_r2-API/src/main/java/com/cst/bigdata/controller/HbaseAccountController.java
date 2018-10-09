package com.cst.bigdata.controller;

import com.cst.bigdata.domain.hbase.UserInfo;
import com.cst.bigdata.service.hbase.HbaseAccountInfoService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import springfox.documentation.annotations.ApiIgnore;

/**
 * @author Johnney.chiu
 * create on 2017/11/23 16:27
 * @Description 测试案例
 */
@RestController
@RequestMapping("/users")
@ApiIgnore
public class HbaseAccountController {
    private final static String TABLE_NAME = "user";

    private final static String FAMILY_INFO = "info";

    @Autowired
    private HbaseAccountInfoService hbaseAccountInfoService;

    @RequestMapping(value="/find/{rowkey}", method= RequestMethod.GET)
    @ResponseBody
    public UserInfo findUserInfoByName(@PathVariable String rowkey){
        UserInfo userInfo = hbaseAccountInfoService.findUserInfoByEntity(TABLE_NAME, FAMILY_INFO, rowkey, new UserInfo());
        return userInfo;
    }

    @RequestMapping(value="/put/{rowkey}", method= RequestMethod.PUT)
    @ResponseBody
    public String putAccount(@PathVariable String rowkey,@RequestBody UserInfo userInfo){
         hbaseAccountInfoService.putAccount(TABLE_NAME, FAMILY_INFO, rowkey, userInfo);
        return "OK";
    }



}

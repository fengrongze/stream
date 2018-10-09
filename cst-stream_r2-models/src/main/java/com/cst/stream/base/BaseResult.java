package com.cst.stream.base;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.*;

/**
 * @author Johnney.chiu
 * create on 2017/12/12 15:15
 * @Description 结果数据
 */
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
@ApiModel
public class BaseResult<T> {

    @ApiModelProperty(value ="调用接口成功")
    private boolean success=false;

    @ApiModelProperty(value ="接口状态码")
    private int code;

    @ApiModelProperty(value = "描述")
    private String description;

    @ApiModelProperty(value = "数据")
    private T data;


    public  static <T> BaseResult success(T data){
        return new BaseResult(true,CodeStatus.SUCCESS_CODE,"success", data);
    }
    public  static <T> BaseResult success(){
        return new BaseResult(true,CodeStatus.SUCCESS_CODE,"success", null);
    }
    public  static  BaseResult fail(int code,String description){
        return new BaseResult(false, code,description, null);
    }

}

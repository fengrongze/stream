package com.cst.stream.stathour;

import com.cst.stream.base.CodeStatus;
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
public class CstStreamBaseResult<T extends CSTData> {

    @ApiModelProperty(value ="调用接口成功")
    private boolean success=false;

    @ApiModelProperty(value ="接口状态码")
    private int code;

    @ApiModelProperty(value = "描述")
    private String description;

    @ApiModelProperty(value = "数据")
    private T data;


    public  static <T extends CSTData> CstStreamBaseResult success(T data){
        return new CstStreamBaseResult(true, CodeStatus.SUCCESS_CODE,"success", data);
    }
    public  static  CstStreamBaseResult success(){
        return new CstStreamBaseResult(true,CodeStatus.SUCCESS_CODE,"success", null);
    }
    public  static CstStreamBaseResult fail(int code, String description){
        return new CstStreamBaseResult(false, code,description, null);
    }

}

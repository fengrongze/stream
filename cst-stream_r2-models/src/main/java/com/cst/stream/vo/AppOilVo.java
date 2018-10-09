package com.cst.stream.vo;

import com.fasterxml.jackson.annotation.JsonFormat;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.*;

import java.util.Date;

/**
 * @author Johnney.chiu
 * create on 2017/12/12 15:22
 * @Description 描述App oil的数据
 */
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
@ApiModel
public class AppOilVo {


    private String id;

    @ApiModelProperty(value = "城市")
    private String province;

    @ApiModelProperty(value = "90号汽油价格")
    private Double b90;

    @ApiModelProperty(value = "93号汽油价格")
    private Double b93;

    @ApiModelProperty(value = "97号汽油价格")
    private Double b97;

    @ApiModelProperty(value = "0号汽油价格")
    private Double b0;

    @ApiModelProperty(value = "时间")
    @JsonFormat(pattern="yyyy-MM-dd HH:mm:ss",timezone = "GMT+8")
    private Date insertTimestamp;

    private String remark;

}

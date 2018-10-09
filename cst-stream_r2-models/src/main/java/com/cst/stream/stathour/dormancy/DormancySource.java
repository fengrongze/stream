package com.cst.stream.stathour.dormancy;

import com.cst.stream.stathour.CSTData;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.*;

/**
 * @author Johnney.Chiu
 * create on 2018/8/9 20:11
 * @Description 休眠包
 * @title
 */
@Getter
@Setter
@ToString(callSuper = true)
@NoArgsConstructor
@ApiModel(value = "DormancySource",description = "dormancy source data ",parent = CSTData.class)
public class DormancySource extends CSTData{

    @ApiModelProperty(value = "旧状态")
    private int oldState = 0;

    @ApiModelProperty(value = "新状态")
    private int newState = 0;

    @Builder
    public DormancySource(String carId, Long time, int oldState, int newState) {
        super(carId, time);
        this.oldState = oldState;
        this.newState = newState;
    }
}

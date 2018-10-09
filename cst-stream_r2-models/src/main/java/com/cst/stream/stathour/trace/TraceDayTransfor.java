package com.cst.stream.stathour.trace;

import com.cst.stream.stathour.CSTData;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.*;

/**
 * @author Johnney.Chiu
 * create on 2018/4/12 15:53
 * @Description 轨迹transfor
 * @title
 */


@Getter
@Setter
@NoArgsConstructor
@ToString(callSuper = true)
@ApiModel(value = "TraceDayTransfor",description = "trace 天数据",parent = CSTData.class)
public class TraceDayTransfor extends CSTData {

    @ApiModelProperty(value = "轨迹条数")
    private Integer traceCounts = 0;


    @Builder
    public TraceDayTransfor(String carId, Long time, Integer traceCounts) {
        super(carId, time);
        this.traceCounts = traceCounts;
    }
}
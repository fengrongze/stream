package com.cst.stream.stathour.tracedelete;

import com.cst.stream.stathour.CSTData;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.*;

/**
 * @author Johnney.Chiu
 * create on 2018/4/12 16:19
 * @Description 估计删除小时源数据
 * @title
 */
@Getter
@Setter
@NoArgsConstructor
@ToString(callSuper = true)
@ApiModel(value = "TraceDeleteHourSource",description = "trace delete 源数据",parent = CSTData.class)
public class TraceDeleteHourSource extends CSTData {

    @ApiModelProperty(value = "din_时间戳")
    private String dinData = "";

    @ApiModelProperty(value = "trace id")
    private String traceId = "";


    @Builder
    public TraceDeleteHourSource(String carId, Long time, String dinData, String traceId) {
        super(carId, time);
        this.dinData = dinData;
        this.traceId = traceId;
    }
}

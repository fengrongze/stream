package com.cst.stream.stathour.gps;

import com.cst.stream.stathour.CSTData;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.*;

/**
 * @author Johnney.chiu
 * create on 2017/12/5 15:25
 * @Description gps day data transfor
 */
@Getter
@Setter
@ToString(callSuper = true)
@NoArgsConstructor
@ApiModel(value = "GpsDayTransfor",description = "gps 天数据结果",parent = CSTData.class)
public class GpsDayTransfor extends CSTData {

    @ApiModelProperty(value = "最大搜星数")
    private Integer maxSatelliteNum=0;

    @ApiModelProperty(value = "gps上报数")
    private Integer gpsCount=0;


    @ApiModelProperty(value = "是否本地")
    private Integer isNonLocal=0;

    @Builder
    public GpsDayTransfor(String carId, Long time, Integer maxSatelliteNum, Integer gpsCount, Integer isNonLocal) {
        this.carId = carId;
        this.time = time;
        this.maxSatelliteNum = maxSatelliteNum;
        this.gpsCount = gpsCount;
        this.isNonLocal = isNonLocal;
    }




}

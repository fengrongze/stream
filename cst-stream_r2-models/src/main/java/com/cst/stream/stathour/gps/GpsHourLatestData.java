package com.cst.stream.stathour.gps;

import com.cst.stream.stathour.CSTData;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.*;

/**
 * @author Johnney.chiu
 * create on 2017/11/27 14:54
 * @Description gps中间计算源
 */
@Getter
@Setter
@ToString(callSuper = true)
@NoArgsConstructor
@ApiModel(value = "GpsHourLatestData",description = "gps hour latest data",parent = CSTData.class)
public class GpsHourLatestData extends CSTData {


    @ApiModelProperty(value = "最大搜星数")
    private Integer maxSatelliteNum=0;

    @ApiModelProperty(value = "gps上报数")
    private Integer gpsCount=0;

    @ApiModelProperty(value = "是否本地")
    private Integer isNonLocal=0;


    @Builder
    public GpsHourLatestData(String carId, Long time, Integer maxSatelliteNum, Integer gpsCount, Integer isNonLocal) {
        super(carId, time);
        this.maxSatelliteNum = maxSatelliteNum;
        this.gpsCount = gpsCount;
        this.isNonLocal = isNonLocal;
    }
}

package com.cst.stream.stathour.gps;

import com.cst.stream.stathour.CSTData;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.*;

/**
 * @author Johnney.chiu
 * create on 2018/05/07 15:25
 * @Description gps day data transfor
 */
@Getter
@Setter
@ToString(callSuper = true)
@NoArgsConstructor
@ApiModel(value = "GpsYearTransfor",description = "gps 年数据结果",parent = CSTData.class)
public class GpsYearTransfor extends CSTData {

    @ApiModelProperty(value = "最大搜星数")
    private Integer maxSatelliteNum=0;

    @ApiModelProperty(value = "gps上报数")
    private Integer gpsCount=0;

    @ApiModelProperty(value = "本地天数")
    private Integer nonLocalNum=0;

    @Builder
    public GpsYearTransfor(String carId, Long time, Integer maxSatelliteNum, Integer gpsCount,  Integer nonLocalNum) {
        this.carId = carId;
        this.time = time;
        this.maxSatelliteNum = maxSatelliteNum;
        this.gpsCount = gpsCount;
        this.nonLocalNum = nonLocalNum;
    }




}

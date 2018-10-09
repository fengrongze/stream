package com.cst.stream.stathour.gps;

import com.cst.stream.stathour.CSTData;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.*;

/**
 * @author Johnney.chiu
 * create on 2017/11/27 14:45
 * @Description 存储于缓存和hbase中的数据
 */
@Getter
@Setter
@ToString(callSuper = true)
@NoArgsConstructor
@ApiModel(value = "GpsHourTransfor",description = "gps 小时数据结果",parent = CSTData.class)
public class GpsHourTransfor extends CSTData {


    //最大搜星数
    @ApiModelProperty(value = "最大搜星数")
    private Integer maxSatelliteNum=0;

    //gps上报数
    @ApiModelProperty(value = "gps上报数")
    private Integer gpsCount=0;


    @ApiModelProperty(value = "是否本地")
    private Integer isNonLocal=0;

    @Builder
    public GpsHourTransfor(String carId, Long time, Integer maxSatelliteNum, Integer gpsCount,  Integer isNonLocal) {
        super(carId, time);
        this.maxSatelliteNum = maxSatelliteNum;
        this.gpsCount = gpsCount;
        this.isNonLocal = isNonLocal;
    }
}

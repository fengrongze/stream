package com.cst.stream.stathour.gps;

import com.cst.stream.stathour.CSTData;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.*;

/**
 * @author Johnney.chiu
 * create on 2017/11/27 14:54
 * @Description gps来源数据
 */
@Getter
@Setter
@ToString(callSuper = true)
@NoArgsConstructor
@ApiModel(value = "GpsHourSource",description = "gps 源数据",parent = CSTData.class)
public class GpsHourSource extends CSTData {

    @ApiModelProperty(value = "搜星数")
    private Integer satellites=0;
    @ApiModelProperty(value = "经度")
    private Double longitude=0.0d;
    @ApiModelProperty(value = "纬度")
    private Double latitude=0.0d;

    @Builder
    public GpsHourSource(String carId, Long time, Integer satellites, Double longitude, Double latitude) {
        super(carId,time);
        this.time = time;
        this.satellites = satellites;
        this.longitude = longitude;
        this.latitude = latitude;
    }

}

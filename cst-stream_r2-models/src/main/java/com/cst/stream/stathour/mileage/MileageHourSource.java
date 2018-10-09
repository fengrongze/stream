package com.cst.stream.stathour.mileage;

import com.cst.stream.stathour.CSTData;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.*;

/**
 * @author Johnney.Chiu
 * create on 2018/8/30 17:47
 * @Description mileage source
 * @title
 */
@Getter
@Setter
@ToString(callSuper = true)
@NoArgsConstructor
@ApiModel(value = "MileageHourSource",description = "mileage 源数据",parent = CSTData.class)
public class MileageHourSource extends CSTData{

    @ApiModelProperty(value = "车gps speed")
    private Integer gpsSpeed=0;

    @ApiModelProperty(value = "车obd speed")
    private Integer obdSpeed=0;

    @ApiModelProperty(value = "gps上报总里程")
    private Double milGpsTotalDistance=0D;

    @ApiModelProperty(value = "obd上报总里程")
    private Double milObdTotalDistance=0D;

    @ApiModelProperty(value = "总油耗")
    private Double milTotalFuel=0D;

    @ApiModelProperty(value = "总行驶时间")
    private Long milRunTotalTime=0L;

    @ApiModelProperty(value = "仪表盘车里程")
    private Double panelDistance=0D;

    @Builder
    public MileageHourSource(String carId, Long time, Integer gpsSpeed, Integer obdSpeed, Double milGpsTotalDistance, Double milObdTotalDistance, Double milTotalFuel, Long milRunTotalTime, Double panelDistance) {
        super(carId, time);
        this.gpsSpeed = gpsSpeed;
        this.obdSpeed = obdSpeed;
        this.milGpsTotalDistance = milGpsTotalDistance;
        this.milObdTotalDistance = milObdTotalDistance;
        this.milTotalFuel = milTotalFuel;
        this.milRunTotalTime = milRunTotalTime;
        this.panelDistance = panelDistance;
    }
}

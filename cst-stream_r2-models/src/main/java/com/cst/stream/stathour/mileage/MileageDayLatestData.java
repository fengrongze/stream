package com.cst.stream.stathour.mileage;

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
@ApiModel(value = "MileageDayLatestData",description = "mileage 天中间数据",parent = CSTData.class)
public class MileageDayLatestData extends CSTData {

    @ApiModelProperty(value = "obd最大速度")
    private Float milObdMaxSpeed=0f;

    @ApiModelProperty(value = "gps最大速度")
    private Float milGpsMaxSpeed=0f;

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

    @ApiModelProperty(value = "行驶时间")
    private Long milRunTotalTime=0L;

    @ApiModelProperty(value = "仪表盘车里程")
    private Double panelDistance=0D;

    @ApiModelProperty(value = "仪表盘车里程标记")
    private Integer panelFlag=0;

    @Builder
    public MileageDayLatestData(String carId, Long time, Float milObdMaxSpeed, Float milGpsMaxSpeed, Integer gpsSpeed, Integer obdSpeed, Double milGpsTotalDistance, Double milObdTotalDistance, Double milTotalFuel, Long milRunTotalTime, Double panelDistance, Integer panelFlag) {
        super(carId, time);
        this.milObdMaxSpeed = milObdMaxSpeed;
        this.milGpsMaxSpeed = milGpsMaxSpeed;
        this.gpsSpeed = gpsSpeed;
        this.obdSpeed = obdSpeed;
        this.milGpsTotalDistance = milGpsTotalDistance;
        this.milObdTotalDistance = milObdTotalDistance;
        this.milTotalFuel = milTotalFuel;
        this.milRunTotalTime = milRunTotalTime;
        this.panelDistance = panelDistance;
        this.panelFlag = panelFlag;
    }
}

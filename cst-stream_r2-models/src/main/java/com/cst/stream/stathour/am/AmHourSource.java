package com.cst.stream.stathour.am;

import com.cst.stream.stathour.CSTData;
import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonTypeName;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.*;

/**
 * @author Johnney.chiu
 * create on 2017/11/27 18:17
 * @Description am的数据源
 */
@Getter
@Setter
@ToString(callSuper = true)
@NoArgsConstructor
@ApiModel(value = "AmHourSource",description = "am 小时数据 ",parent = CSTData.class)
public class AmHourSource extends CSTData {


    @ApiModelProperty(value = "告警类型")
    private Integer alarmType=0;

    @ApiModelProperty(value = "故障码")
    private String troubleCode;

    @ApiModelProperty(value = "标记类型")
    private Integer gatherType=0;

    @ApiModelProperty(value = "经度")
    private Double longitude=0d;

    @ApiModelProperty(value = "纬度")
    private Double latitude=0d;

    @ApiModelProperty(value = "版本类型")
    private Integer versionType=0;

    @ApiModelProperty(value = "拔出时长")
    private Long pullout=0L;

    @Builder
    public AmHourSource(String carId,  Long time, Integer alarmType, String troubleCode, Integer gatherType,
                        Double longitude, Double latitude, Integer versionType,Long pullout) {
        super(carId,time);
        this.time = time;
        this.alarmType = alarmType;
        this.troubleCode = troubleCode;
        this.gatherType = gatherType;
        this.longitude = longitude;
        this.latitude = latitude;
        this.versionType = versionType;
        this.pullout=pullout;
    }


}

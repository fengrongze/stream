package com.cst.bigdata.domain.mybatis.validatedata;

import com.cst.stream.stathour.am.AmHourTransfor;
import com.cst.stream.stathour.de.DeHourTransfor;
import com.cst.stream.stathour.gps.GpsHourTransfor;
import com.cst.stream.stathour.obd.ObdHourTransfor;
import com.cst.stream.stathour.trace.TraceHourTransfor;
import com.cst.stream.stathour.tracedelete.TraceDeleteHourTransfor;
import com.cst.stream.stathour.voltage.VoltageHourTransfor;
import lombok.*;

/**
 * @author Johnney.Chiu
 * create on 2018/5/18 15:23
 * @Description
 * @title
 */
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
@ToString
public class IntegrateHourVo {

    private ObdHourTransfor obdHourTransfor;

    private GpsHourTransfor gpsHourTransfor;

    private AmHourTransfor amHourTransfor;

    private DeHourTransfor deHourTransfor;

    private TraceHourTransfor traceHourTransfor;

    private TraceDeleteHourTransfor traceDeleteHourTransfor;

    private VoltageHourTransfor voltageHourTransfor;

    private String[] originalData;

    private IntegrateHourVo originalHourVo;

}

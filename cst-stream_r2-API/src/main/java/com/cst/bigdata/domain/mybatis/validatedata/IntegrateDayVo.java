package com.cst.bigdata.domain.mybatis.validatedata;

import com.cst.stream.stathour.am.AmDayTransfor;
import com.cst.stream.stathour.de.DeDayTransfor;
import com.cst.stream.stathour.gps.GpsDayTransfor;
import com.cst.stream.stathour.obd.ObdDayTransfor;
import com.cst.stream.stathour.trace.TraceDayTransfor;
import com.cst.stream.stathour.tracedelete.TraceDeleteDayTransfor;
import com.cst.stream.stathour.voltage.VoltageDayTransfor;
import lombok.*;

/**
 * @author Johnney.Chiu
 * create on 2018/5/21 15:27
 * @Description
 * @title
 */

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
@ToString
public class IntegrateDayVo {


    private ObdDayTransfor obdDayTransfor;

    private GpsDayTransfor gpsDayTransfor;

    private AmDayTransfor amDayTransfor;

    private DeDayTransfor deDayTransfor;

    private TraceDayTransfor traceDayTransfor;

    private TraceDeleteDayTransfor traceDeleteDayTransfor;

    private VoltageDayTransfor voltageDayTransfor;

    private String[] originalData;

    private IntegrateDayVo integrateDayVo;
}

package com.cst.bigdata.service.hbase;

import com.cst.base.factory.GdcpFactory;
import com.cst.base.inf.GdcpRInterface;
import com.cst.base.inf.NormalTable;
import com.cst.gdcp.di.DataPackage;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Date;
import java.util.List;

/**
 * @author Johnney.Chiu
 * create on 2018/6/22 15:46
 * @Description bigdaoData
 * @title
 */
@Slf4j
@Service
public class CstDataFromBigDaoService {

    @Autowired
    private GdcpFactory gdcpFactory;

    public List<DataPackage> getObdData(String din,Date from,Date to){
        List<DataPackage> list = null;
        GdcpRInterface gdcpRInterface=gdcpFactory.getNormalDao(NormalTable.OBD);
        try {
            list=gdcpRInterface.load(din, from, to);
        } catch (Throwable throwable) {
            log.error("get from big dao error {}",din,throwable);
        }
        return list;
    }
    public List<DataPackage> getGpsData(String din,Date from,Date to){
        List<DataPackage> list = null;
        GdcpRInterface gdcpRInterface=gdcpFactory.getNormalDao(NormalTable.GPS);
        try {
            list=gdcpRInterface.load(din, from, to);
        } catch (Throwable throwable) {
            log.error("get from big dao error {}",din,throwable);
        }
        return list;
    }
    public List<DataPackage> getAlarmData(String din,Date from,Date to){
        List<DataPackage> list = null;
        GdcpRInterface gdcpRInterface=gdcpFactory.getNormalDao(NormalTable.ALARM);
        try {
            list=gdcpRInterface.load(din, from, to);
        } catch (Throwable throwable) {
            log.error("get from big dao error {}",din,throwable);
        }
        return list;
    }
    public List<DataPackage> getDriveEventData(String din,Date from,Date to){
        List<DataPackage> list = null;
        GdcpRInterface gdcpRInterface=gdcpFactory.getNormalDao(NormalTable.EVENT);
        try {
            list=gdcpRInterface.load(din, from, to);
        } catch (Throwable throwable) {
            log.error("get from big dao error {}",din,throwable);
        }
        return list;
    }
    public List<DataPackage> getTraceData(String din,Date from,Date to){
        List<DataPackage> list = null;
        GdcpRInterface gdcpRInterface=gdcpFactory.getNormalDao(NormalTable.TRAVEL);
        try {
            list=gdcpRInterface.load(din, from, to);
        } catch (Throwable throwable) {
            log.error("get from big dao error {}",din,throwable);
        }
        return list;
    }

    public List<DataPackage> getVoltageData(String din,Date from,Date to){
        List<DataPackage> list = null;
        GdcpRInterface gdcpRInterface=gdcpFactory.getNormalDao(NormalTable.VOLTAGE_GATHER);
        try {
            list=gdcpRInterface.load(din, from, to);
        } catch (Throwable throwable) {
            log.error("get from big dao error {}",din,throwable);
        }
        return list;
    }
}

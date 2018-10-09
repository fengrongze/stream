package com.cst.bigdata.service.dubbo;

import com.alibaba.dubbo.config.annotation.Reference;
import com.cst.cmds.car.query.service.CarQueryService;
import com.cst.cmds.exception.BusinessException;
import com.cst.hfrq.dto.car.CarInfo;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * @author Johnney.Chiu
 * create on 2018/6/22 15:34
 * @Description
 * @title
 */
@Slf4j
@Service
public class CarInfoService {

    @Reference
    private CarQueryService carService;

    public CarInfo getCarInfoFromRPC(String carId){
        CarInfo info = null;
        try {
            info=carService.selectOne(carId);
        } catch (BusinessException e) {
            log.error("get data from dubbo CarQueryService error:{}",carId,e);
        }
        return info;
    }


    public List<CarInfo> getCarInfoFromRPC(List<String> carIds){
        List<CarInfo> carInfos = null;
        try {
            carInfos=carInfos=carService.selectList(carIds);
        } catch (BusinessException e) {
            log.error("get data from dubbo CarQueryService error:{}",carIds,e);
        }
        return carInfos;
    }

}

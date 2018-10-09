package com.cst.bigdata.controller.streamvalidate;

import com.cst.bigdata.controller.BaseController;
import com.cst.bigdata.domain.mybatis.validatedata.IntegrateDayVo;
import com.cst.bigdata.domain.mybatis.validatedata.IntegrateHourVo;
import com.cst.bigdata.service.integrate.StreamValidateIntegrateService;
import com.cst.stream.base.BaseResult;
import com.cst.stream.common.DateTimeUtil;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.FileSystemResource;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;
import springfox.documentation.annotations.ApiIgnore;

import javax.validation.constraints.NotNull;
import java.io.File;
import java.text.ParseException;
import java.util.Date;
import java.util.List;

/**
 * @author Johnney.Chiu
 * create on 2018/5/18 14:47
 * @Description validate data
 * @title
 */
@RestController
@RequestMapping("/validate")
@Api(description = "validate stream data")
@Slf4j
public class ValidateDataController extends BaseController {


    private final static String HOUR_NAME_TRANSFOR = "hour_data_com_%d";
    private final static String HOUR_NAME_PER_TRANSFOR = "hour_data_per_com_%d";
    private final static String HOUR_NAME_PER_ORIGINAL_TRANSFOR = "hour_data_per_com_original_%d";
    private final static String DAY_NAME_TRANSFOR = "day_data_com_%d";
    private final static String DAY_NAME_PER_TRANSFOR = "day_data_per_com_%d";
    private final static String DAY_NAME_PER_ORIGINAL_TRANSFOR = "day_data_per_com_original_%d";

    private final static String ORIGINAL_NAME = "orignal_%d_%s";


    @Autowired
    private StreamValidateIntegrateService streamValidateIntegrateService;

    @ApiOperation(value = "get hour integrate data", httpMethod = "POST")
    @PostMapping(value = "/hour/get/integrate/data")
    @ResponseBody
    public ResponseEntity<FileSystemResource> getHourIntegrateData(@ApiParam(value="validate file") @RequestParam("validateFile") MultipartFile file){
        if(file.isEmpty())
            return null;
        String fileName = file.getOriginalFilename();
        log.info("上传的文件名为：" + fileName);
        String suffixName = fileName.substring(fileName.lastIndexOf("."));
        log.info("上传的后缀名为：" + suffixName);
        String trueName = fileName.substring(0, fileName.indexOf("."));
        /*if(appCarMachineMileIntegratedService.fixDataWithFile(file))
            return BaseResult.success("fix data successfully!");
        return BaseResult.fail(CodeStatus.GENERAL_ERROR_CODE, fileName);*/

        String originalName = String.format(ORIGINAL_NAME, System.currentTimeMillis(), file.getOriginalFilename());

        List<IntegrateHourVo> list = streamValidateIntegrateService.getHourDatasFromFile(true,file, originalName, trueName);
        String myFileName =String.format(HOUR_NAME_TRANSFOR,System.currentTimeMillis());
        File exportFile = streamValidateIntegrateService.writeAndExportHourData(list, myFileName);
        return export(exportFile);
    }

    @ApiOperation(value = "get hour integrate data with per", httpMethod = "POST")
    @PostMapping(value = "/hour/get/integratewithper/data")
    @ResponseBody
    public ResponseEntity<FileSystemResource> getHourIntegrateDataWithPer(@ApiParam(value="validate file") @RequestParam("validateFile") MultipartFile file){
        if(file.isEmpty())
            return null;
        String fileName = file.getOriginalFilename();
        log.info("上传的文件名为：" + fileName);
        String suffixName = fileName.substring(fileName.lastIndexOf("."));
        log.info("上传的后缀名为：" + suffixName);
        String trueName = fileName.substring(0, fileName.indexOf("."));
        /*if(appCarMachineMileIntegratedService.fixDataWithFile(file))
            return BaseResult.success("fix data successfully!");
        return BaseResult.fail(CodeStatus.GENERAL_ERROR_CODE, fileName);*/
        
        String originalName = String.format(ORIGINAL_NAME, System.currentTimeMillis(), file.getOriginalFilename());

        List<IntegrateHourVo> list = streamValidateIntegrateService.getHourDatasFromFile(true,file, originalName, trueName);
        String myFileName =String.format(HOUR_NAME_PER_TRANSFOR,System.currentTimeMillis());
        File exportFile = streamValidateIntegrateService.writeAndExportHourDataWithPer(list, myFileName);
        return export(exportFile);
    }


    @ApiOperation(value = "get hour integrate data with per from original data", httpMethod = "POST")
    @PostMapping(value = "/hour/get/integratewithperfromOriginal/data")
    @ResponseBody
    public ResponseEntity<FileSystemResource> getHourIntegrateDataWithPerFromOrignal(@ApiParam(value="validate file") @RequestParam("validateFile") MultipartFile file){
        if(file.isEmpty())
            return null;
        String fileName = file.getOriginalFilename();
        log.info("上传的文件名为：" + fileName);
        String suffixName = fileName.substring(fileName.lastIndexOf("."));
        log.info("上传的后缀名为：" + suffixName);
        String trueName = fileName.substring(0, fileName.indexOf("."));
        /*if(appCarMachineMileIntegratedService.fixDataWithFile(file))
            return BaseResult.success("fix data successfully!");
        return BaseResult.fail(CodeStatus.GENERAL_ERROR_CODE, fileName);*/

        String originalName = String.format(ORIGINAL_NAME, System.currentTimeMillis(), file.getOriginalFilename());

        List<IntegrateHourVo> list = streamValidateIntegrateService.getHourDatasFromFileWithOriginal(true,file, originalName, trueName);
        String myFileName =String.format(HOUR_NAME_PER_ORIGINAL_TRANSFOR,System.currentTimeMillis());
        File exportFile = streamValidateIntegrateService.writeAndExportHourDataWithOriginalDataPer(list, myFileName);
        return export(exportFile);
    }

    @ApiOperation(value = "get hour original data", httpMethod = "POST")
    @PostMapping(value = "/hour/get/original/data")
    @ResponseBody
    public ResponseEntity<FileSystemResource> getHourOriginalData(@ApiParam(value="origin file") @RequestParam("origin") MultipartFile file){
        if(file.isEmpty())
            return null;
        String fileName = file.getOriginalFilename();
        log.info("上传的文件名为：" + fileName);
        String suffixName = fileName.substring(fileName.lastIndexOf("."));
        log.info("上传的后缀名为：" + suffixName);
        String trueName = fileName.substring(0, fileName.indexOf("."));
        /*if(appCarMachineMileIntegratedService.fixDataWithFile(file))
            return BaseResult.success("fix data successfully!");
        return BaseResult.fail(CodeStatus.GENERAL_ERROR_CODE, fileName);*/

        String originalName = String.format(ORIGINAL_NAME, System.currentTimeMillis(), file.getOriginalFilename());

        List<IntegrateHourVo> list = streamValidateIntegrateService.getHourDatasFromFile(false,file, originalName, trueName);
        String myFileName =String.format(HOUR_NAME_TRANSFOR,System.currentTimeMillis());
        File exportFile = streamValidateIntegrateService.writeAndExportHourData(list, myFileName);
        return export(exportFile);
    }


    @ApiOperation(value = "get day integrate data", httpMethod = "POST")
    @PostMapping(value = "/day/get/integrate/data")
    @ResponseBody
    public ResponseEntity<FileSystemResource> getDayIntegrateData(@ApiParam(value="validate file") @RequestParam("validateFile") MultipartFile file){
        if(file.isEmpty())
            return null;
        String fileName = file.getOriginalFilename();
        log.info("上传的文件名为：" + fileName);
        String suffixName = fileName.substring(fileName.lastIndexOf("."));
        log.info("上传的后缀名为：" + suffixName);
        String trueName = fileName.substring(0, fileName.indexOf("."));
        /*if(appCarMachineMileIntegratedService.fixDataWithFile(file))
            return BaseResult.success("fix data successfully!");
        return BaseResult.fail(CodeStatus.GENERAL_ERROR_CODE, fileName);*/

        String originalName = String.format(ORIGINAL_NAME, System.currentTimeMillis(), file.getOriginalFilename());

        List<IntegrateDayVo> list = streamValidateIntegrateService.getDayDatasFromFile(true,file, originalName, trueName);
        String myFileName =String.format(DAY_NAME_TRANSFOR,System.currentTimeMillis());
        File exportFile = streamValidateIntegrateService.writeAndExportDayData(list, myFileName);
        return export(exportFile);
    }

    @ApiOperation(value = "get day integrate per data", httpMethod = "POST")
    @PostMapping(value = "/day/get/integratewithper/data")
    @ResponseBody
    public ResponseEntity<FileSystemResource> getDayIntegrateDataWithPer(@ApiParam(value="validate file") @RequestParam("validateFile") MultipartFile file){
        if(file.isEmpty())
            return null;
        String fileName = file.getOriginalFilename();
        log.info("上传的文件名为：" + fileName);
        String suffixName = fileName.substring(fileName.lastIndexOf("."));
        log.info("上传的后缀名为：" + suffixName);
        String trueName = fileName.substring(0, fileName.indexOf("."));
        /*if(appCarMachineMileIntegratedService.fixDataWithFile(file))
            return BaseResult.success("fix data successfully!");
        return BaseResult.fail(CodeStatus.GENERAL_ERROR_CODE, fileName);*/

        String originalName = String.format(ORIGINAL_NAME, System.currentTimeMillis(), file.getOriginalFilename());

        List<IntegrateDayVo> list = streamValidateIntegrateService.getDayDatasFromFile(true,file, originalName, trueName);
        String myFileName =String.format(DAY_NAME_PER_TRANSFOR,System.currentTimeMillis());
        File exportFile = streamValidateIntegrateService.writeAndExportDayDataWithPer(list, myFileName);
        return export(exportFile);
    }

    @ApiOperation(value = "get day integrate per data", httpMethod = "POST")
    @PostMapping(value = "/day/get/integrateperfromOriginal/data")
    @ResponseBody
    public ResponseEntity<FileSystemResource> getDayIntegrateDataWithPerFromOrignal(@ApiParam(value="validate file") @RequestParam("validateFile") MultipartFile file){
        if(file.isEmpty())
            return null;
        String fileName = file.getOriginalFilename();
        log.info("上传的文件名为：" + fileName);
        String suffixName = fileName.substring(fileName.lastIndexOf("."));
        log.info("上传的后缀名为：" + suffixName);
        String trueName = fileName.substring(0, fileName.indexOf("."));
        /*if(appCarMachineMileIntegratedService.fixDataWithFile(file))
            return BaseResult.success("fix data successfully!");
        return BaseResult.fail(CodeStatus.GENERAL_ERROR_CODE, fileName);*/




        String originalName = String.format(ORIGINAL_NAME, System.currentTimeMillis(), file.getOriginalFilename());

        List<IntegrateDayVo> list = streamValidateIntegrateService.getDayDatasFromFileWithOriginal(true,file, originalName, trueName);
        String myFileName =String.format(DAY_NAME_PER_ORIGINAL_TRANSFOR,System.currentTimeMillis());
        File exportFile = streamValidateIntegrateService.writeAndExportDayDataWithPerAndOriginal(list, myFileName);
        return export(exportFile);
    }



    @ApiOperation(value = "get day original data", httpMethod = "POST")
    @PostMapping(value = "/day/get/original/data")
    @ResponseBody
    public ResponseEntity<FileSystemResource> getDayOriginalData(@ApiParam(value="origin file") @RequestParam("origin") MultipartFile file){
        if(file.isEmpty())
            return null;
        String fileName = file.getOriginalFilename();
        log.info("上传的文件名为：" + fileName);
        String suffixName = fileName.substring(fileName.lastIndexOf("."));
        log.info("上传的后缀名为：" + suffixName);
        String trueName = fileName.substring(0, fileName.indexOf("."));
        /*if(appCarMachineMileIntegratedService.fixDataWithFile(file))
            return BaseResult.success("fix data successfully!");
        return BaseResult.fail(CodeStatus.GENERAL_ERROR_CODE, fileName);*/

        String originalName = String.format(ORIGINAL_NAME, System.currentTimeMillis(), file.getOriginalFilename());

        List<IntegrateDayVo> list = streamValidateIntegrateService.getDayDatasFromFile(false,file, originalName, trueName);
        String myFileName =String.format(DAY_NAME_TRANSFOR,System.currentTimeMillis());
        File exportFile = streamValidateIntegrateService.writeAndExportDayData(list, myFileName);
        return export(exportFile);
    }

    @ApiIgnore
    @ApiOperation(value = "get original data", httpMethod = "POST")
    @PostMapping(value = "/get/datas/carIds/date")
    @ResponseBody
    public BaseResult<IntegrateHourVo> getSomeCarIdsWithtime(
            @ApiParam(value = "carIds", required = true) @RequestParam(defaultValue = "")  String[]  carIds,
            @ApiParam(value = "时间 yyyy-MM-dd HH", required = true) @RequestParam("time") @NotNull @DateTimeFormat(pattern = "yyyy-MM-dd HH") Date dateTime){

        return BaseResult.success(streamValidateIntegrateService.getHourDatasFromCarIds(true, dateTime.getTime(), carIds));
    }

    @ApiIgnore
    @ApiOperation(value = "get  data", httpMethod = "POST")
    @PostMapping(value = "/get/datas/carIds/dateStr")
    @ResponseBody
    public BaseResult<IntegrateHourVo> getDataWitCarIdsAndDateStr(
            @ApiParam(value = "carIds", required = true) @RequestParam(defaultValue = "")  String[]  carIds,
            @ApiParam(value = "时间 yyyy-MM-dd HH", required = true) @RequestParam("time") @NotNull String dateStr){

        long times ;
        try {
            times = DateTimeUtil.strToTimestamp(dateStr, "yyyy-MM-dd HH");
            log.info("times :{}",times);
            return BaseResult.success(streamValidateIntegrateService.getHourDatasFromCarIds(true, times, carIds));
        } catch (ParseException e) {
            log.info("errr",e);
            return BaseResult.fail(0,"tranfor error");
        }

    }
}

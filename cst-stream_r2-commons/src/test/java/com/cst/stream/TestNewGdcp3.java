package com.cst.stream;

import com.cst.gdcp.factory.kafka.dto.car.BaseCarDto;
import com.cst.gdcp.factory.kafka.dto.din.*;
import com.cst.stream.common.JsonHelper;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.reflect.TypeToken;
import org.junit.Test;

/**
 * @author Johnney.Chiu
 * create on 2018/8/8 19:22
 * @Description
 * @title
 */
public class TestNewGdcp3 {

    @Test
    public void parseObd(){
        String obddata = "{\"fid\":18,\"data\":{\"acc\":0,\"rspd\":20,\"mful\":40,\"ail\":17,\"ignta\":-64,\"b1s2c\":-126.08,\"cspd\":-1,\"egrt\":48624,\"airp\":50,\"envm\":-10,\"obdt\":-1,\"mil\":-1,\"tqp\":-1,\"engld\":0,\"ignt\":-1,\"b1s1v\":0,\"milt\":1,\"airfl\":0,\"dtm\":1533722113,\"mask\":\"26d7fffff0\",\"engl\":0.25,\"fulp\":0,\"tnum\":0,\"crt\":1533722096,\"tmil\":-1,\"clt\":11,\"b1s1c\":-128,\"temil\":10,\"mmil\":3000,\"rfulp\":0,\"fulg1\":-95,\"b1s2v\":0,\"volt\":-1,\"mfp\":16,\"fuld\":52.199997,\"swa\":3276.8,\"thp\":1.9,\"pdp\":460.80002,\"rful\":-1,\"tful\":-1,\"gear\":-1},\"din\":\"M201712010002\",\"ptm\":1533722115891,\"carShortId\":97684335003441618,\"sn\":3,\"dtm\":1533722113,\"carId\":\"58197ec7fad246ca8fe582416da00301\"}";
        Gson gson = new Gson();
        BaseCarDto<ObdDto> baseDto = gson.fromJson(obddata, new TypeToken<BaseCarDto<ObdDto>>() {
        }.getType());

        System.out.println(baseDto);

        obddata = "{\"fid\":16,\"data\":{\"rspd\":20,\"pdop\":1,\"gtm\":1533722096,\"amil\":4.0,\"lng\":\"106.553029\",\"utc\":1533722096,\"spd\":50,\"pmod\":1,\"ista\":1,\"drt\":74.4,\"alt\":18,\"adrt\":1000,\"cspd\":60,\"hdop\":1.1,\"mtyp\":1,\"vld\":1,\"ptyp\":0,\"aful\":0.0,\"satl\":5,\"volt\":12.849999,\"vdop\":1.2,\"lat\":\"29.567469\"},\"din\":\"M201712010002\",\"ptm\":1533722099095,\"carShortId\":97684335003441618,\"sn\":2,\"dtm\":1533722096,\"carId\":\"58197ec7fad246ca8fe582416da00301\"}";
        BaseCarDto<GpsDto> gpsDtoBaseCarDto=gson.fromJson(obddata, new TypeToken<BaseCarDto<GpsDto>>() {
        }.getType());
        System.out.println(gpsDtoBaseCarDto);

        obddata = "{\"fid\":19,\"data\":{\"obdt\":17,\"sec\":{\"acc\":1,\"deb\":1,\"bmil\":1,\"rspd\":1,\"vin1\":1,\"auto\":1,\"cspd\":1,\"acl\":1,\"sun\":1,\"oilw\":1,\"wid\":1,\"mb\":1,\"drb\":1,\"grd\":1,\"drf\":1,\"fbrk\":1,\"dlb\":1,\"lrb\":1,\"rfuel\":1,\"mask\":\"ffffffffffffffffffff\",\"pbrk\":1,\"dlf\":1,\"eng\":1,\"lrf\":1,\"rev\":1,\"llb\":1,\"read\":1,\"wrb\":1,\"llf\":1,\"rvm\":1,\"wrf\":1,\"fulw\":1,\"trk\":1,\"wlb\":1,\"uph\":1,\"lrt\":1,\"wlf\":1,\"mmil\":1,\"warn\":1,\"bfog\":1,\"horn\":1,\"airc\":1,\"llt\":1,\"wiper\":1,\"ffog\":1,\"lwh\":1,\"gear\":15},\"bf\":{\"sht2s\":7,\"cst\":1,\"slt3g\":13,\"rmd\":2,\"ght1n\":6,\"ghts\":30,\"ost\":2,\"glt1n\":1,\"glts\":10,\"sht3g\":18,\"sht3s\":8,\"slt3s\":3,\"ghtg\":21,\"ag\":1,\"gltg\":1,\"slt4g\":14,\"iv\":110,\"glt\":10,\"ght\":60,\"ght2n\":7,\"prt\":-5,\"glt2n\":2,\"volt\":0.11,\"sht4g\":19,\"sht4s\":9,\"slv\":1,\"slt4s\":4,\"shv\":1.2,\"psf\":0,\"rt\":-10,\"mcst\":2,\"ght3\":35,\"sht1g\":16,\"pst\":2,\"ght4\":45,\"glt1\":-35,\"ght3n\":8,\"glt2\":-25,\"glt3\":-15,\"glt3n\":3,\"oc\":130,\"glt4\":-5,\"slt1g\":11,\"slt1s\":1,\"mask\":\"ffffffffffffffffffff\",\"sht4\":15,\"sht3\":14,\"slt2\":7,\"ap2\":80,\"slt1\":6,\"slvs\":30,\"cf\":0,\"ap1\":40,\"sht1s\":6,\"sht2\":13,\"sht1\":12,\"shvs\":50,\"ov\":24,\"slt4\":9,\"slt3\":8,\"ght1\":15,\"ght2\":25,\"ght4n\":9,\"sht2g\":17,\"shvg\":20,\"glt4n\":4,\"slt2g\":12,\"slt2s\":2,\"tr\":1,\"slvg\":10},\"of\":{\"bmsv\":0.1,\"bc\":-2000,\"lbv\":110,\"fst\":0,\"soc\":0.29999998,\"isr\":0,\"arpc\":10,\"cspd\":55,\"airp\":1000,\"egrt\":1000,\"bcst\":0,\"soh\":0.5,\"bp\":-319955,\"mil\":0,\"bv\":120,\"apc\":0.12100001,\"milt\":1,\"bpd\":0.040000003,\"bwst\":0,\"mxdc\":-3192,\"rci\":0,\"isfg\":1,\"mxfc\":-3193,\"mtc\":-3190,\"mask\":\"ffffffffffffffff\",\"mct\":-30,\"ee\":0.31,\"cpst\":0,\"bfg\":1,\"mp\":-100,\"tnum\":0,\"crt\":1000,\"hvr\":0,\"tpc\":200,\"mspd\":25,\"tmil\":0.05,\"blt\":40,\"mtt\":-20,\"mtv\":12,\"trm\":65535,\"bht\":60,\"mrs\":1,\"mmil\":2000,\"swst\":0,\"pdp\":0.05},\"dtm\":1533613240,\"mix\":{\"acc\":1,\"rspd\":98,\"mful\":19,\"fulp\":13,\"iful\":0.17999999,\"ail\":-31,\"ignta\":-62,\"est\":1,\"clt\":-30,\"airp\":14,\"engald\":0.17999999,\"envm\":-29,\"tqp\":0.22,\"rfulp\":0.17,\"fulg1\":-98.1,\"engld\":17,\"mfp\":12,\"airfl\":1.5,\"swa\":-3274.5999,\"thp\":1.6,\"rful\":-1,\"mask\":\"ffffffffff\",\"tful\":0.161}},\"din\":\"M201411180010\",\"ptm\":1533721941818,\"carShortId\":97684335003435132,\"sn\":2,\"dtm\":1533613240,\"carId\":\"M201411180010\"}";

        BaseCarDto<EObdDto.Mix> eObdDtoBaseCarDto=gson.fromJson(obddata, new TypeToken<BaseCarDto<EObdDto.Mix>>() {
        }.getType());
        System.out.println((eObdDtoBaseCarDto.getData().getClt()));

        JsonObject jsonObject = (JsonObject) new JsonParser().parse(obddata);
        System.out.println(jsonObject.get("data").getAsJsonObject().get("mix").getAsJsonObject().get("mful").getAsInt());
        obddata =  "{\"fid\":19}";
        System.out.println(jsonObject.get("data").getAsJsonObject().get("mix").getAsJsonObject().get("mful").getAsInt());



        obddata = "{\"fid\":10,\"data\":{\"volt\":12,\"dtm\":1533722169},\"din\":\"M201712010002\",\"ptm\":1533722171990,\"carShortId\":97684335003441618,\"sn\":6,\"dtm\":1533722169,\"carId\":\"58197ec7fad246ca8fe582416da00301\"}";

        BaseCarDto<OtherDto.Volt> otherDtoBaseCarDto=gson.fromJson(obddata, new TypeToken<BaseCarDto<OtherDto.Volt>>() {
        }.getType());

        System.out.println(otherDtoBaseCarDto.getData().getVolt());
        String data = "{\"fid\":33,\"data\":{\"ext\":{\"pltm\":1534227946},\"etyp\":53249,\"gps\":{\"rspd\":1421,\"pdop\":1,\"gtm\":1534228066,\"amil\":0.1,\"lng\":\"106.553029\",\"utc\":1534228065,\"spd\":60,\"pmod\":1,\"ista\":0,\"drt\":74.4,\"alt\":18,\"adrt\":1000,\"cspd\":60,\"hdop\":1.1,\"mtyp\":1,\"vld\":1,\"ptyp\":0,\"aful\":0.0,\"satl\":5,\"volt\":12.849999,\"vdop\":1.2,\"lat\":\"29.567469\"},\"ptyp\":1},\"din\":\"M201712010006\",\"ptm\":1534228067357,\"carShortId\":97684335003441928,\"sn\":2,\"dtm\":1534228065,\"carId\":\"M201712010006\"}";
        jsonObject = (JsonObject) new JsonParser().parse(data);
        System.out.println(jsonObject.get("data").getAsJsonObject().get("ext").getAsJsonObject().get("pltm").getAsLong());

    }

}

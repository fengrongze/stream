package com.cst.stream.stathour;


import lombok.*;

/**
 * @author Johnney.chiu
 * create on 2017/11/30 17:30
 * @Description 定义最后一次上传的数据
 */
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@ToString(callSuper = true)
public class LastTimeData<T extends CSTData> {
    private Long lastTime=0l;

    private T data;

}

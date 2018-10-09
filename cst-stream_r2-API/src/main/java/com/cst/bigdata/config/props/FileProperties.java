package com.cst.bigdata.config.props;

import lombok.*;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * @author Johnney.Chiu
 * create on 2018/4/24 10:18
 * @Description 文件读写
 * @title
 */
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@ToString
@ConfigurationProperties(prefix = "file")
public class FileProperties {

    private String startTime;

    private String stopTime;

    private String filePath;

    private String fileName;

    private String outPath;

}

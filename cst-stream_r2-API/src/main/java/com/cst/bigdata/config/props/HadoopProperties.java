package com.cst.bigdata.config.props;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.List;

/**
 * @author Johnney.chiu
 * create on 2017/11/24 11:16
 * @Description hadoop的属性设置
 */

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@ConfigurationProperties(prefix = "hadoop")
public class HadoopProperties {

    private String hadoopHomeDir;

    private List<String> hadoopCoreFile;

    private String crossPlatform;

    private String ubertask;

}

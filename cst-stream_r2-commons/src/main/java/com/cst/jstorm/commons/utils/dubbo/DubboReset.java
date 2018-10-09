package com.cst.jstorm.commons.utils.dubbo;

import org.springframework.beans.factory.annotation.Value;

/**
 * @author Johnney.Chiu
 * create on 2018/5/22 11:33
 * @Description dubbo config properties
 * @title
 */
public class DubboReset {


    @Value("${dubbo.application.name}")
    private String applicationName;

    @Value("${dubbo.application.owner}")
    private String applicationOwner;

    @Value("${dubbo.application.organization}")
    private String applicationOrganization;

    @Value("${dubbo.registry.address}")
    private String registryAddress;

    @Value("${dubbo.registry.group}")
    private String registryGroup;

    @Value("${dubbo.annotation.package}")
    private String annotationPackage;

    @Value("${dubbo.protocol.name}")
    private String protocolName;

    @Value("${dubbo.protocol.port}")
    private int protocolPort;

    public DubboReset() {
    }

    public String getApplicationName() {
        return applicationName;
    }

    public void setApplicationName(String applicationName) {
        this.applicationName = applicationName;
    }

    public String getApplicationOwner() {
        return applicationOwner;
    }

    public void setApplicationOwner(String applicationOwner) {
        this.applicationOwner = applicationOwner;
    }

    public String getApplicationOrganization() {
        return applicationOrganization;
    }

    public void setApplicationOrganization(String applicationOrganization) {
        this.applicationOrganization = applicationOrganization;
    }

    public String getRegistryAddress() {
        return registryAddress;
    }

    public void setRegistryAddress(String registryAddress) {
        this.registryAddress = registryAddress;
    }

    public String getRegistryGroup() {
        return registryGroup;
    }

    public void setRegistryGroup(String registryGroup) {
        this.registryGroup = registryGroup;
    }

    public String getAnnotationPackage() {
        return annotationPackage;
    }

    public void setAnnotationPackage(String annotationPackage) {
        this.annotationPackage = annotationPackage;
    }

    public String getProtocolName() {
        return protocolName;
    }

    public void setProtocolName(String protocolName) {
        this.protocolName = protocolName;
    }

    public int getProtocolPort() {
        return protocolPort;
    }

    public void setProtocolPort(int protocolPort) {
        this.protocolPort = protocolPort;
    }

    @Override
    public String toString() {
        return "DubboReset{" +
                "applicationName='" + applicationName + '\'' +
                ", applicationOwner='" + applicationOwner + '\'' +
                ", applicationOrganization='" + applicationOrganization + '\'' +
                ", registryAddress='" + registryAddress + '\'' +
                ", registryGroup='" + registryGroup + '\'' +
                ", annotationPackage='" + annotationPackage + '\'' +
                ", protocolName='" + protocolName + '\'' +
                ", protocolPort='" + protocolPort + '\'' +
                '}';
    }
}

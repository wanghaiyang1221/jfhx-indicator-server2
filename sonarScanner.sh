#!/bin/bash
 
# JDK11的目录
JDK11_HOME="/usr/local/jdk-11.0.13"
# SonarQube服务器地址
SONAR_HOST="http://10.60.36.75:9000"
# SonarQube访问令牌
SONAR_TOKEN="squ_b694f6d43068592f8678a095244afefe48bf9c41"
# 项目键，确保唯一性，通常为groupId:artifactId
PROJECT_KEY="com.ouyeelf:jfhx-indicator-server"
# 项目名称，通常为artifactId
PROJECT_NAME="jfhx-indicator-server"
 
####################################### Execute Maven to SonarQube scan ########################################################
# 临时调整JDK的目录，SonarQube扫描时需要使用JDK11及以上版本
export JAVA_HOME=${JDK11_HOME}
# 通过运行Maven插件执行SonarQube代码扫描
mvn clean compile sonar:sonar -Dsonar.host.url=${SONAR_HOST} -Dsonar.login=${SONAR_TOKEN} -Dsonar.projectKey=${PROJECT_KEY} -Dsonar.projectName=${PROJECT_NAME}
 
####################################### View Scan SonarQube's results ########################################################
# 使用curl发送HTTP GET请求获取项目状态信息
response=$(curl -s -u "${SONAR_TOKEN}:" "${SONAR_HOST}/api/qualitygates/project_status?projectKey=${PROJECT_KEY}" | jq -r '.projectStatus.status')
# 项目看板
dashboard=${SONAR_HOST}/dashboard?id=${PROJECT_KEY}
# 检查返回结果是否为"OK"，表示成功
if [[ $response == "ERROR" ]]; then
  echo "SonarQube服务代码扫描不通过，请前往[${dashboard}]查看扫描结果，修改后再执行发布操作"
  exit 1  # 如果项目状态异常，退出并返回非零退出代码
else
  echo "SonarQube服务代码扫描通过"
fi
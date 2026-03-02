# jfhx-indicator-server 项目

## 项目简介
jfhx-indicator-server是一个基于微服务脚手架构建的现代化后端微服务，需要使用JDK17开发和部署项目。

## 技术栈

以下列表主要列举了后端所使用的主要技术栈选型

| 技术          | 版本     | 说明                   |
| ------------- | -------- | ---------------------- |
| Spring Boot   | 2.7.5    | 应用开发框架           |
| Spring Cloud  | 2021.0.5 | 应用开发框架           |
| MySQL         | 5.6      | 关系型数据库           |
| Alibaba Druid | 1.2.15   | Druid数据源            |
| HikariCP      | 4.0.3    | HikariCP数据源         |
| MyBatis       | 3.5.10   | 数据持久框架           |
| MyBatis-Plus  | 3.5.2    | MyBatis增强工具        |
| Redis         | 5.0.8    | 内存存储中间件         |
| Redission     | 3.13.2   | Redis客户端            |
| RocketMQ      | 5.0.0    | RocketMQ消息队列中间件 |
| Nacos         | 2.1.2    | 分布式配置中心         |
| Eureka        | 1.10.17  | 分布式注册中心         |
| Zuul          | 1.3.1    | 微服务网关             |
| OpenFeign     | 3.8.0    | Rest客户端             |
| Resilience4J  | 1.7.0    | 断路器实现             |
| Apache Log4J2 | 2.18.0   | 日志框架               |
| Logback       | 1.2.11   | 日志框架               |
| FastJson      | 1.2.83   | JSON序列化工具         |
| SkyWalking    | 8.7.0    | 链路追踪中间件         |
| ElasticSearch | 7.11.2   | 搜索引擎               |
| xxl-job       | 2.4.0    | 任务调度平台           |

## 项目结构

```
项目根目录/
├── profile/                         # 三方配置文件
│   ├── dev/                         # 本地开发环境配置
│   ├── test/                        # 测试环境配置
│   └── prod/                        # 生产环境配置
├── src/
│   └── main/
│       ├── java/
│       │   └── com.ouyeelf...server
│       │       ├── controller        # 控制器层
│       │       ├── service           # 业务服务层
│       │       │   ├── db            # 数据服务层
│       │       │   ├── biz           # 业务服务层
│       │       │   ├── common        # 公共服务层
│       │       │   ├── job           # 定时服务层
│       │       ├── mapper            # 数据访问层 - MyBatis Mapper接口
│       │       ├── entity            # 实体层 - 数据库映射对象
│       │       ├── vo                # 视图对象层 - 前端展示数据
│       │       ├── api               # 外部API接口层
│       │       │   ├── request       # API请求参数
│       │       │   └── response      # API响应参数
│       │       ├── config            # 配置层
│       │       ├── runner            # 启动器 - CommandLineRunner
│       │       ├── util              # 工具类
│       │       ├── filter            # 过滤器
│       │       ├── converter         # 对象转换器
│       └── resources/
│           ├── mapper                # MyBatis XML配置文件目录
│           ├── static                # 静态资源文件目录
│           ├── templates             # 模板文件目录
│           ├── application.yml       # 主配置文件
│           ├── application-dev.yml   # 开发环境配置文件
│           ├── application-test.yml  # 测试环境配置文件
│           └── application-prod.yml  # 生产环境配置文件
├── target/                           # 编译输出目录
└── pom.xml                           # Maven依赖管理文件
```
注：如果使用了Nacos配置中心，则需要将application.yml名称调整为bootstrap.yml，并将其它yml文件同步至配置中心后删除

## 三方配置文件
敏感配置（如证书、密钥等）存放在profile目录：
```
profile/
├── dev/
│   ├── ssl/           # SSL证书
│   ├── payment/       # 支付配置
│   └── sms/           # 短信服务配置
├── test/
│   └── ...           # 测试环境配置
└── prod/
    └── ...           # 生产环境配置
```

## 打包
### 结构化方式
```bash

# 测试环境：会在target目录下生成xxx-test-1.0-SNAPSHOT.zip部署包
mvn clean package -DskipTests -Ptest

# 生产环境：会在target目录下生成xxx-prod-1.0-SNAPSHOT.zip部署包
mvn clean package -DskipTests -Pprod
```

### Docker方式

使用Docker部署方式之前，需要配置仓库地址，具体需要参考开发手册

```bash

# 构建测试环境镜像并上传仓库
mvn clean package -Ptest docker:build
# 推送镜像
mvn docker:push

# 构建生产环境镜像并上传仓库
mvn clean package -Ptest docker:build
# 推送镜像
mvn docker:push

# 构建测试环境Dockerfile文件以及docker-compose.yml文件，存放在target/docker目录下
mvn clean docker:create -Ptest

# 构建生产环境Dockerfile文件以及docker-compose.yml文件，存放在target/docker目录下
mvn clean docker:create -Pprod
```

### 部署

### 结构化方式
 - 完整部署：将target目录下的**.zip包上传至服务器，注：第一次部署时需要手动解压，使用unzip -o xxx.zip解压目录
 - 快速部署：将target目录下的jfhx-indicator-server.jar包上传至服务器，将其上传并覆盖到已解压的jfhx-indicator-server/lib目录下

```
# 进入解压的bin目录下
cd jfhx-indicator-server/bin

# 完整部署：自动备份 + 自动解压 + 重启
sh deploy.sh

# 快速部署：重启
sh restart.sh

# 停止服务
sh stop.sh

```

### Docker方式
将target/docker目录下的Dockerfile文件、docker-compose.yml文件上传至服务器指定目录下
```
# 在指定目录下运行如下命令启动服务
docker-compose up -d --build --force-recreate
```

## 开发手册

http://www.ouyeelf.com/doc/development.html
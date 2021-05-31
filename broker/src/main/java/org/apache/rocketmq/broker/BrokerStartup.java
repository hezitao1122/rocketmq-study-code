/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.broker;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.MQVersion;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.common.RemotingUtil;
import org.apache.rocketmq.remoting.common.TlsMode;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.remoting.netty.NettySystemConfig;
import org.apache.rocketmq.remoting.netty.TlsSystemConfig;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.srvutil.ServerUtil;
import org.apache.rocketmq.store.config.BrokerRole;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.rocketmq.remoting.netty.TlsSystemConfig.TLS_ENABLE;

public class BrokerStartup {
    public static Properties properties = null;
    public static CommandLine commandLine = null;
    public static String configFile = null;
    public static InternalLogger log;

    public static void main(String[] args) {
        start(createBrokerController(args));
    }

    public static BrokerController start(BrokerController controller) {
        try {

            controller.start();

            String tip = "The broker[" + controller.getBrokerConfig().getBrokerName() + ", "
                + controller.getBrokerAddr() + "] boot success. serializeType=" + RemotingCommand.getSerializeTypeConfigInThisServer();

            if (null != controller.getBrokerConfig().getNamesrvAddr()) {
                tip += " and name server is " + controller.getBrokerConfig().getNamesrvAddr();
            }

            log.info(tip);
            System.out.printf("%s%n", tip);
            return controller;
        } catch (Throwable e) {
            e.printStackTrace();
            System.exit(-1);
        }

        return null;
    }

    public static void shutdown(final BrokerController controller) {
        if (null != controller) {
            controller.shutdown();
        }
    }
    /** description: 构建 BrokerController
     * 1. 设置默认网络通信相关的数据
     * 2. 解析通过命令行传递的参数
     * 3. 初始化配置broker的核心配置信息
     *      1). brokerConfig  broker的配置信息
     *      2). nettyServerConfig netty服务器的配置信息
     *      3). nettyClientConfig netty客户端的配置信息
     *  4. 设置是否使用TLS配置
     *  5. 设置用于消息存储的配置信息类
     *  6. 如果当broker是slave的话,需要进行一些额外的配置
     *  7. -c 命令代表你要加载的配置文件的地址,此时它会读取你的配置文件,并进行加载
     *  8. 将-c加载的 一些命令行的配置信息覆盖到 brokerConfig里面
     *  9. 检查ROCKETMQ_HOME环境变量
     *  10. 获取NameServer的地址列表,然后进行解析
     *  11. 判断broker的角色信息,针对不同角色做不同的处理
     *  12. 判断是否基于dleger技术来管理主从同步和commitlog,如果是则设置为-1
     *  13. 设置HA监听端口号
     *  14. 配置日志和打印参数相关
     *  15. 构建BrokerController核心组件
     *  16. 执行初始化BrokerController
     *         1). 加载一些磁盘上的数据进内存之中
     *             (1). Topic配置
     *             (2). Consumer的消费offset
     *             (3). Consumer订阅组\过滤器
     *             (4). 加载完成后,result则是true
     *         2). 加载成功后执行以下逻辑
     *             (1). 首先创建了消息存储管理组件
     *             (2). 初始化并启动dleger技术进行主从同步以及管理commitlog
     *             (3). 初始化broker的统计组件
     *         3. 加载netty服务的信息
     *         4. 初始化一堆线程,如请求处理(sendMessageExecutor)、
     *             (1). pullMessageExecutor消息拉取、
     *             (2). replyMessageExecutor 回复消息、
     *             (3). queryMessageExecutor 查询消息、
     *             (4). adminBrokerExecutor  管理broker、
     *             (5). clientManageExecutor  管理客户端、
     *             (6). heartbeatExecutor    发送心跳、
     *             (7). endTransactionExecutor  结束事务、
     *             (8). consumerManageExecutor  管理consumer线程池
     *         5. 开始定时调度一些后台线程执行
     *             (1). getBrokerStats broker统计任务
     *             (2). consumerOffsetManager 定时进行consumer消费,offset持久化到磁盘的任务
     *             (3). consumerFilterManager 定时对consumer filter过滤器进行持久化的任务
     *             (4). protectBroker  定时进行broker保护的任务
     *             (5). getMessageStore 进行落后的connitlog分发的任务
     *  17. 注册一个JVM关闭的钩子,去调用BrokerController的关闭信息
     * @param args 命令行传递的参数
     * @return: org.apache.rocketmq.broker.BrokerController
     * @Author: zeryts
     * @email: hezitao@agree.com
     * @Date: 2021/5/31 16:41
     */
    public static BrokerController createBrokerController(String[] args) {

        System.setProperty(RemotingCommand.REMOTING_VERSION_KEY, Integer.toString(MQVersion.CURRENT_VERSION));
        /**
         * 设置默认网络通信相关的数据
         */
        if (null == System.getProperty(NettySystemConfig.COM_ROCKETMQ_REMOTING_SOCKET_SNDBUF_SIZE)) {
            NettySystemConfig.socketSndbufSize = 131072;
        }

        if (null == System.getProperty(NettySystemConfig.COM_ROCKETMQ_REMOTING_SOCKET_RCVBUF_SIZE)) {
            NettySystemConfig.socketRcvbufSize = 131072;
        }

        try {
            //PackageConflictDetect.detectFastjson();
            /**
             * 解析通过命令行传递的参数
             */
            Options options = ServerUtil.buildCommandlineOptions(new Options());
            commandLine = ServerUtil.parseCmdLine("mqbroker", args, buildCommandlineOptions(options),
                new PosixParser());
            if (null == commandLine) {
                System.exit(-1);
            }
            /**
             * 初始化配置broker的核心配置信息
             *  brokerConfig  broker的配置信息
             *  nettyServerConfig netty服务器的配置信息
             *  nettyClientConfig netty客户端的配置信息
             */
            final BrokerConfig brokerConfig = new BrokerConfig();
            final NettyServerConfig nettyServerConfig = new NettyServerConfig();
            final NettyClientConfig nettyClientConfig = new NettyClientConfig();
            /**
             * 设置是否使用TLS配置
             */
            nettyClientConfig.setUseTLS(Boolean.parseBoolean(System.getProperty(TLS_ENABLE,
                String.valueOf(TlsSystemConfig.tlsMode == TlsMode.ENFORCING))));
            /**
             * 设置了netty服务器的监听端口号
             */
            nettyServerConfig.setListenPort(10911);
            /**
             * 看名字像是用于消息存储的配置信息类
             */
            final MessageStoreConfig messageStoreConfig = new MessageStoreConfig();
            /**
             * 如果当broker是slave的话,需要进行一些额外的配置
             */
            if (BrokerRole.SLAVE == messageStoreConfig.getBrokerRole()) {
                int ratio = messageStoreConfig.getAccessMessageInMemoryMaxRatio() - 10;
                messageStoreConfig.setAccessMessageInMemoryMaxRatio(ratio);
            }
            /**
             * -c 命令代表你要加载的配置文件的地址,此时它会读取你的配置文件,并进行加载
             *
             */
            if (commandLine.hasOption('c')) {
                String file = commandLine.getOptionValue('c');
                if (file != null) {
                    configFile = file;
                    InputStream in = new BufferedInputStream(new FileInputStream(file));
                    properties = new Properties();
                    properties.load(in);

                    properties2SystemEnv(properties);
                    MixAll.properties2Object(properties, brokerConfig);
                    MixAll.properties2Object(properties, nettyServerConfig);
                    MixAll.properties2Object(properties, nettyClientConfig);
                    MixAll.properties2Object(properties, messageStoreConfig);

                    BrokerPathConfigHelper.setBrokerConfigPath(file);
                    in.close();
                }
            }
            /**
             * 将一些命令行的配置信息覆盖到 brokerConfig里面
             */
            MixAll.properties2Object(ServerUtil.commandLine2Properties(commandLine), brokerConfig);
            /**
             * 检查ROCKETMQ_HOME环境变量
             */
            if (null == brokerConfig.getRocketmqHome()) {
                System.out.printf("Please set the %s variable in your environment to match the location of the RocketMQ installation", MixAll.ROCKETMQ_HOME_ENV);
                System.exit(-2);
            }
            /**
             * 获取NameServer的地址列表,然后进行解析
             */
            String namesrvAddr = brokerConfig.getNamesrvAddr();
            if (null != namesrvAddr) {
                try {
                    String[] addrArray = namesrvAddr.split(";");
                    for (String addr : addrArray) {
                        RemotingUtil.string2SocketAddress(addr);
                    }
                } catch (Exception e) {
                    System.out.printf(
                        "The Name Server Address[%s] illegal, please set it as follows, \"127.0.0.1:9876;192.168.0.1:9876\"%n",
                        namesrvAddr);
                    System.exit(-3);
                }
            }
            /**
             * 判断broker的角色信息,针对不同角色做不同的处理
             */
            switch (messageStoreConfig.getBrokerRole()) {
                case ASYNC_MASTER:
                case SYNC_MASTER:
                    brokerConfig.setBrokerId(MixAll.MASTER_ID);
                    break;
                case SLAVE:
                    if (brokerConfig.getBrokerId() <= 0) {
                        System.out.printf("Slave's brokerId must be > 0");
                        System.exit(-3);
                    }

                    break;
                default:
                    break;
            }
            /**
             * 判断是否基于dleger技术来管理主从同步和commitlog,如果是则设置为-1
             */
            if (messageStoreConfig.isEnableDLegerCommitLog()) {
                brokerConfig.setBrokerId(-1);
            }
            /**
             * 设置HA监听端口号
             */
            messageStoreConfig.setHaListenPort(nettyServerConfig.getListenPort() + 1);
            /**
             * 配置日志相关
             */
            LoggerContext lc = (LoggerContext) LoggerFactory.getILoggerFactory();
            JoranConfigurator configurator = new JoranConfigurator();
            configurator.setContext(lc);
            lc.reset();
            configurator.doConfigure(brokerConfig.getRocketmqHome() + "/conf/logback_broker.xml");

            /**
             * -p 是打印启动参数
             * -m 打印配置参数
             *
             */
            if (commandLine.hasOption('p')) {
                InternalLogger console = InternalLoggerFactory.getLogger(LoggerName.BROKER_CONSOLE_NAME);
                MixAll.printObjectProperties(console, brokerConfig);
                MixAll.printObjectProperties(console, nettyServerConfig);
                MixAll.printObjectProperties(console, nettyClientConfig);
                MixAll.printObjectProperties(console, messageStoreConfig);
                System.exit(0);
            } else if (commandLine.hasOption('m')) {
                InternalLogger console = InternalLoggerFactory.getLogger(LoggerName.BROKER_CONSOLE_NAME);
                MixAll.printObjectProperties(console, brokerConfig, true);
                MixAll.printObjectProperties(console, nettyServerConfig, true);
                MixAll.printObjectProperties(console, nettyClientConfig, true);
                MixAll.printObjectProperties(console, messageStoreConfig, true);
                System.exit(0);
            }
            /**
             * 打印broker的配置参数
             */
            log = InternalLoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);
            MixAll.printObjectProperties(log, brokerConfig);
            MixAll.printObjectProperties(log, nettyServerConfig);
            MixAll.printObjectProperties(log, nettyClientConfig);
            MixAll.printObjectProperties(log, messageStoreConfig);
            /**
             * 构建BrokerController核心组件
             */
            final BrokerController controller = new BrokerController(
                brokerConfig,
                nettyServerConfig,
                nettyClientConfig,
                messageStoreConfig);
            // remember all configs to prevent discard
            controller.getConfiguration().registerConfig(properties);

            /**
             * 初始化BrokerController
             */
            boolean initResult = controller.initialize();
            if (!initResult) {
                controller.shutdown();
                System.exit(-3);
            }

            /**
             * 注册一个JVM关闭的钩子,去调用BrokerController的关闭信息
             */
            Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
                private volatile boolean hasShutdown = false;
                private AtomicInteger shutdownTimes = new AtomicInteger(0);

                @Override
                public void run() {
                    synchronized (this) {
                        log.info("Shutdown hook was invoked, {}", this.shutdownTimes.incrementAndGet());
                        if (!this.hasShutdown) {
                            this.hasShutdown = true;
                            long beginTime = System.currentTimeMillis();
                            controller.shutdown();
                            long consumingTimeTotal = System.currentTimeMillis() - beginTime;
                            log.info("Shutdown hook over, consuming total time(ms): {}", consumingTimeTotal);
                        }
                    }
                }
            }, "ShutdownHook"));

            return controller;
        } catch (Throwable e) {
            e.printStackTrace();
            System.exit(-1);
        }

        return null;
    }

    private static void properties2SystemEnv(Properties properties) {
        if (properties == null) {
            return;
        }
        String rmqAddressServerDomain = properties.getProperty("rmqAddressServerDomain", MixAll.WS_DOMAIN_NAME);
        String rmqAddressServerSubGroup = properties.getProperty("rmqAddressServerSubGroup", MixAll.WS_DOMAIN_SUBGROUP);
        System.setProperty("rocketmq.namesrv.domain", rmqAddressServerDomain);
        System.setProperty("rocketmq.namesrv.domain.subgroup", rmqAddressServerSubGroup);
    }

    private static Options buildCommandlineOptions(final Options options) {
        Option opt = new Option("c", "configFile", true, "Broker config properties file");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("p", "printConfigItem", false, "Print all config item");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("m", "printImportantConfig", false, "Print important config item");
        opt.setRequired(false);
        options.addOption(opt);

        return options;
    }
}

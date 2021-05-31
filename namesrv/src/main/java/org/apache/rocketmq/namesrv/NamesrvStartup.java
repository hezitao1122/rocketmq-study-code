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
package org.apache.rocketmq.namesrv;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import ch.qos.logback.core.joran.spi.JoranException;
import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.Callable;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.rocketmq.common.MQVersion;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.common.namesrv.NamesrvConfig;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.srvutil.ServerUtil;
import org.apache.rocketmq.srvutil.ShutdownHookThread;
import org.slf4j.LoggerFactory;

public class NamesrvStartup {
    /**
     * 和日志、配置、命令行有关的参数
     */
    private static InternalLogger log;
    private static Properties properties = null;
    private static CommandLine commandLine = null;

    public static void main(String[] args) {
        main0(args);
    }

    public static NamesrvController main0(String[] args) {

        try {
            NamesrvController controller = createNamesrvController(args);
            start(controller);
            String tip = "The Name Server boot success. serializeType=" + RemotingCommand.getSerializeTypeConfigInThisServer();
            log.info(tip);
            System.out.printf("%s%n", tip);
            return controller;
        } catch (Throwable e) {
            e.printStackTrace();
            System.exit(-1);
        }

        return null;
    }

    /** description: 创建一个NameServer的Controller,基于Netty
     * 作用为接受所有http请求
     * @param args
     * @return: org.apache.rocketmq.namesrv.NamesrvController
     * @Author: zeryts
     * @email: hezitao@agree.com
     * @Date: 2021/5/31 8:52
     */
    public static NamesrvController createNamesrvController(String[] args) throws IOException, JoranException {
        System.setProperty(RemotingCommand.REMOTING_VERSION_KEY, Integer.toString(MQVersion.CURRENT_VERSION));
        //PackageConflictDetect.detectFastjson();

        Options options = ServerUtil.buildCommandlineOptions(new Options());
        /**
            构建一些和命令行相关的参数
         */
        commandLine = ServerUtil.parseCmdLine("mqnamesrv", args, buildCommandlineOptions(options), new PosixParser());
        if (null == commandLine) {
            System.exit(-1);
            return null;
        }
        /**
         * 构建配置
         * Namesrv应该是NameServer自己的配置信息
         * NtttyServerConfig代表NameServer是通过Netty进行通信的
         * 9876 代表NameServer默认监听的端口号
         */
        final NamesrvConfig namesrvConfig = new NamesrvConfig();
        final NettyServerConfig nettyServerConfig = new NettyServerConfig();
        nettyServerConfig.setListenPort(9876);
        /**
         * 如果命令行戴上了 -c 这个选项,就会自动去加载配置文件
         */
        if (commandLine.hasOption('c')) {
            String file = commandLine.getOptionValue('c');
            if (file != null) {
                InputStream in = new BufferedInputStream(new FileInputStream(file));
                properties = new Properties();
                /**
                 * 将配置文件加载进一个properties里面
                 */
                properties.load(in);
                /**
                 * 将读取的配置文件放到核心配置文件里面去
                 */
                MixAll.properties2Object(properties, namesrvConfig);
                MixAll.properties2Object(properties, nettyServerConfig);

                namesrvConfig.setConfigStorePath(file);

                System.out.printf("load config properties file OK, %s%n", file);
                in.close();
            }
        }
        /**
         * 查看命令行是否有带 -p选项
         * 有的话,打印出你的所有配置信息
         */
        if (commandLine.hasOption('p')) {
            InternalLogger console = InternalLoggerFactory.getLogger(LoggerName.NAMESRV_CONSOLE_NAME);
            MixAll.printObjectProperties(console, namesrvConfig);
            MixAll.printObjectProperties(console, nettyServerConfig);
            System.exit(0);
        }
        /**
         * 将命令行读取的配置加载到namesrvConfig中去,进行覆盖
         * 也就是 命令行>配置文件>默认配置
         */
        MixAll.properties2Object(ServerUtil.commandLine2Properties(commandLine), namesrvConfig);

        /**
         * 查看是否设置了环境变量  ROCKET_MQ_HOME
         */
        if (null == namesrvConfig.getRocketmqHome()) {
            System.out.printf("Please set the %s variable in your environment to match the location of the RocketMQ installation%n", MixAll.ROCKETMQ_HOME_ENV);
            System.exit(-2);
        }

        LoggerContext lc = (LoggerContext) LoggerFactory.getILoggerFactory();
        JoranConfigurator configurator = new JoranConfigurator();
        configurator.setContext(lc);
        lc.reset();
        configurator.doConfigure(namesrvConfig.getRocketmqHome() + "/conf/logback_namesrv.xml");

        log = InternalLoggerFactory.getLogger(LoggerName.NAMESRV_LOGGER_NAME);

        MixAll.printObjectProperties(log, namesrvConfig);
        MixAll.printObjectProperties(log, nettyServerConfig);

        final NamesrvController controller = new NamesrvController(namesrvConfig, nettyServerConfig);

        // remember all configs to prevent discard
        controller.getConfiguration().registerConfig(properties);

        return controller;
    }
    /** description: 启动NameServer
        1. 初始化NamesrvController
     *      1). 加载一些kv的配置
     *      2). 初始化netty服务器
     *          (1). 传入的是netty服务器的一些配置
     *      3). 设置netty服务器的工作线程池
     *      4). 把工作线程池给netty服务器
     *      5). 启动一个定时任务,定时检查Broker心跳
     *      6). 这个是和FileWatch相关的
     *  2. 注册一个jvm关闭的shutdown钩子,jvm关闭的时候会执行上述注册的回调函数
     *      1). 调用NamesrvController的shutdown()方法
     *      2). 调用remotingServer的shutdown()方法,关闭Netty相关的配置
     *      3). 调用remotingExecutor的shutdown()方法
     *  3. 启动Netty服务
     *      1). 基于Netty的API去配置Netty的网络信息
     *      2). 设置了一大堆网络请求处理器,只要netty服务器收到一个请求,那么就会依次使用下面的处理器来处理请求
     *          (1). handshakeHandler  负责握手
     *          (2). NettyDecoder      负责编码解码
     *          (3). IdleStateHandler  负责连接空闲管理
     *          (4). connectionManageHandler 负责网络连接管理
     *          (5). serverHandler    负责网络请求处理
     *   3). 启动netty服务,并且绑定和监听端口
     * @param controller 上面构造和配置出来的NamesrvController
     * @return: org.apache.rocketmq.namesrv.NamesrvController
     * @Author: zeryts
     * @email: hezitao@agree.com
     * @Date: 2021/5/31 15:30
     */
    public static NamesrvController start(final NamesrvController controller) throws Exception {


        if (null == controller) {
            throw new IllegalArgumentException("NamesrvController is null");
        }
        /**
         * 对NamesrvController进行初始化
         */
        boolean initResult = controller.initialize();
        if (!initResult) {
            controller.shutdown();
            System.exit(-3);
        }
        /**
         * 注册一个jvm关闭的shutdown钩子,jvm关闭的时候会执行上述注册的回调函数
         */
        Runtime.getRuntime().addShutdownHook(new ShutdownHookThread(log, new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                controller.shutdown();
                return null;
            }
        }));
        /**
         * 启动netty服务
         */
        controller.start();

        return controller;
    }

    public static void shutdown(final NamesrvController controller) {
        controller.shutdown();
    }

    public static Options buildCommandlineOptions(final Options options) {
        Option opt = new Option("c", "configFile", true, "Name server config properties file");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("p", "printConfigItem", false, "Print all config item");
        opt.setRequired(false);
        options.addOption(opt);

        return options;
    }

    public static Properties getProperties() {
        return properties;
    }
}

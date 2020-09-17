# Keeper v0.6.3
Keeper是一个轻量级但功能强大的任务调度工具，目标是满足业务任何类型的调度需求，支持数据统计分析、大数据计算、后端开发等各种应用场景。

官方网站：[www.qross.io](http://www.qross.io)

在线演示：[www.qross.cn:8080](http://www.qross.cn:8080)

联系作者：<wu@qross.io>

稳定版本：v0.6.3-RELEASE


## 调度 Job
1. 支持时间计划调度、无限循环调度、仅依赖触发的调度、仅手工执行的调度
2. 时间计划调度使用标准Cron表达式配置，支持配置多个Cron表达式，如9点30和10点45执行。
3. 无限循环调度支持分时段设置间隔时间，如工作时间间隔5秒执行一次，休息时间间隔10分钟执行一次。
4. 支持重启时自动补全停机过程中错过执行的任务。
5. 支持按时自动启用和停用调度。
6. 支持手工修改下一次任务运行时间。
7. 支持任务多并行度设置。
8. 支持手工立即运行任务。
9. 可以设置调度的负责人，用于权限控制和预警。

## 工作流 DAG
1. 支持调度Shell命令、PQL计算过程和Python脚本。
2. DAG的节点可以是不同的命令或脚本类型。
3. 可以启停用DAG中的某个节点和后续节点。
4. 单个命令和脚本可以设置超时时间，超时时会自动中断任务。
5. 单个命令和脚本可以设置失败或超时重试次数。
6. 可以向命令和脚本传递环境参数，如任务时间、调度ID等

## 依赖 Dependency
1. 支持3种类型的依赖：任务依赖、SQL查询依赖和PQL依赖。
2. 通过任务前置依赖可以构建调度之间的“DAG”。
3. 通过后置依赖可以对数据计算的结果正确性进行检查。
4. 通过PQL依赖可以定制任何类型的依赖，如接口依赖、文件依赖等。
5. 通过依赖可以扩展调度的分支，如上游任务成功和失败时下游执行不同的调度任务。

## 事件 Event
1. 任务在生命周期中有不同的状态，在某一个状态点可触发不同的事件。通过事件可以实现预警功能。
2. 可触发事件的状态包括：新建任务、任务依赖检查超过限定、任务准备好、任务成功、任务失败、任务超时、任务结果不正确。
3. 基本事件功能包含发邮件、请求接口和执行PQL过程。
5. 任务失败、任务超时、任务结果不正确都可以设置延时重启并可设置重启上限。
6. 可以设置一个阈值，超时会触发任务执行缓慢事件，但不会中断任务执行。
7. 可以通过PQL自定义事件，如拨打语音电话、发短信、发送企业微信消息、发送钉钉消息等。
8. 预警信息可以同时发送给团队负责人和管理员。
9. 可以设置事件触发的场景，比如手工启动的任务不触发事件。

## 任务 Task
1. 支持重启已完成的任务和中断正在执行的任务。
2. 记录每一次任务的运行情况，每个历史记录都可看到。
3. 异常任务提醒和检查功能。
4. 瀑布流图可以直观的显示整个任务和单个DAG的执行情况。
5. 更易读的彩色日志。
6. 可以只查看Debug日志和出错日志。 
7. 可以只查看DAG中某一个节点的日志。

## 权限 Role
1. 多角色设置，可精细控制每个角色的权限。
2. 普通用户只能管理自己的调度。
3. 团队负责人可以管理自己和团队的所有调度。
4. 可以控制团队负责人和普通用户的权限直至只读。
5. 可以控制调度管理员的权限范围。
6. 有访客角色，仅拥有有限浏览权限。

## 管理 Management
1. 调度可按项目分组管理
2. 丰富的统计数据和图表，实时掌控任务运行情况。
3. 可以自动清理过期的任务运行记录。
4. 可以在页面上查看各组件的心跳并重启Keeper。
5. 可以在页面上查看Keeper的运行日志和出错日志。
6. 中英双语管理界面，自由切换。
7. 32个随机颜色主题，每次换个心情。也可固定颜色主题。

## 扩展 PQL
1. 与其他调度工具最大的区别是支持PQL数据处理语言，不仅支持运行PQL计算过程，而且可以通过PQL对Keeper的各项功能进行扩展。
2. Shell命令中支持嵌入PQL变量和表达式，以实现命令的定制化传参。
3. 通过PQL依赖可以定制任何类型的依赖，如接口依赖、文件依赖等。
4. 事件和预警中也支持运行PQL，并且可以通过PQL自定义事件来实现更丰富的预警功能，如短信、电话、企业微信、钉钉等。通过PQL和事件，可以对整个调度任务的生命周期进行精确控制。
5. 正在开发PQL触发器，可以通过PQL对Keeper进行无限扩展，实现各种定制功能。
6.  提供 Restful 接口，可以对创建任务并对任务进行控制。

## 未来功能 Future
1. PQL触发器，控制整个Keeper运行流程的每一个细节，预计v0.6.5实现。
2. DAG以图形化的方式进行显示和管理，预计v0.6.6实现。
3. 调度之间的血缘关系图和失败任务一键重启，预计v0.6.6实现。
4. 集群化，预计v0.7.0实现。
......


* Keeper的管理工具Master暂时未开源，如有需要请访问官网或联系作者。
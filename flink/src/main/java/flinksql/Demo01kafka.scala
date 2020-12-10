package flinksql

import java.time.Duration

import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.{CheckpointConfig, ExecutionCheckpointingOptions}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment


/**
 * create by young
 * date:20/12/6
 * desc:
 */
object Demo01kafka {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = StreamTableEnvironment.create(env)

    // 失败重启,固定间隔,每隔3秒重启1次,总尝试重启10次
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(10, 3))
    // 本地测试线程 1
    env.setParallelism(1)
    // 事件处理的时间，由系统时间决定
    //    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
    // checkpoint 设置
    val tableConfig = tEnv.getConfig.getConfiguration
    // 开启checkpoint
    tableConfig.set(ExecutionCheckpointingOptions.CHECKPOINTING_MODE, CheckpointingMode.EXACTLY_ONCE)
    // checkpoint的超时时间周期，1 分钟做一次checkpoint, 每次checkpoint 完成后 sink 才会执行
    tableConfig.set(ExecutionCheckpointingOptions.CHECKPOINTING_INTERVAL, Duration.ofSeconds(60))
    // checkpoint的超时时间, 检查点一分钟内没有完成将被丢弃
    tableConfig.set(ExecutionCheckpointingOptions.CHECKPOINTING_TIMEOUT, Duration.ofSeconds(60))
    // checkpoint 最小间隔，两个检查点之间至少间隔 30 秒
    tableConfig.set(ExecutionCheckpointingOptions.MIN_PAUSE_BETWEEN_CHECKPOINTS, Duration.ofSeconds(30))
    // 同一时间只允许进行一个检查点
    tableConfig.set(ExecutionCheckpointingOptions.MAX_CONCURRENT_CHECKPOINTS, Integer.valueOf(1))
    // 手动cancel时是否保留checkpoint
    tableConfig.set(ExecutionCheckpointingOptions.EXTERNALIZED_CHECKPOINT, CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)



    //  {"id":1,"name":"laowang","age":19}
    //kafkaTable
    val kafkaTable =
      """
        |CREATE TABLE kafkatable (
        |    id int,
        |    name string,
        |    age int
        |)
        |WITH (
        |    'connector' = 'kafka',
        |    'topic' = 'test01',
        |    'scan.startup.mode' = 'latest-offset',
        |    'properties.bootstrap.servers' = 'jinghang02:9092',
        |    'properties.group.id' = 'test',
        |    'format' = 'json'
        |)
  """.stripMargin

    tEnv.executeSql(kafkaTable)
    tEnv.executeSql("select * from kafkatable").print()

  }
}

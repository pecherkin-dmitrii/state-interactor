package neoflex

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.Types
import org.apache.flink.api.java.ExecutionEnvironment
import org.apache.flink.api.java.io.CsvOutputFormat
import org.apache.flink.configuration.Configuration
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.state.api.Savepoint
import org.apache.flink.state.api.functions.KeyedStateReaderFunction
import org.apache.flink.state.api.functions.KeyedStateReaderFunction.Context
import org.apache.flink.util.Collector
import org.apache.flink.api.java.tuple.{Tuple2 => FlinkTuple2}

import scala.collection.JavaConverters._
import scala.util.Try

object StateInteractor extends App {
  val fieldDelimiter = ";"
  val params = getParams(args)
  val env = ExecutionEnvironment.getExecutionEnvironment
  val backEnd = new RocksDBStateBackend(params.rocksDbStateBackendUri)
  val savepoint = Savepoint.load(env, params.savepointPath, backEnd)

  val listKeyedState = savepoint.readKeyedState(params.operatorUid, new ListStateReaderFunction)
  listKeyedState.writeAsCsv(params.valueStateCsvFilePath, CsvOutputFormat.DEFAULT_LINE_DELIMITER, fieldDelimiter)

  val valueKeyedState = savepoint.readKeyedState(params.operatorUid, new ValueStateReaderFunction)
  valueKeyedState.writeAsCsv(params.valueStateCsvFileName, CsvOutputFormat.DEFAULT_LINE_DELIMITER, fieldDelimiter)

  env.execute("state interaction job")

  case class KeyedState(key: String, valuesList: List[String])

  class ListStateReaderFunction extends KeyedStateReaderFunction[String, FlinkTuple2[String, List[String]]] {
    var listState: ListState[String] = _

    override def open(parameters: Configuration): Unit = {
      val listStateDescriptor = new ListStateDescriptor(params.listStateName, Types.STRING)
      listState = getRuntimeContext.getListState(listStateDescriptor)
    }

    override def readKey(
                          key: String,
                          ctx: Context,
                          out: Collector[FlinkTuple2[String, List[String]]]): Unit = {
      val data = new FlinkTuple2(key, listState.get.asScala.toList)
      out.collect(data)
    }
  }

  class ValueStateReaderFunction extends KeyedStateReaderFunction[String, FlinkTuple2[String, String]] {
    var valueState: ValueState[String] = _

    override def open(parameters: Configuration): Unit = {
      val valueStateDescriptor = new ValueStateDescriptor(params.valueStateName, Types.STRING)
      valueState = getRuntimeContext.getState(valueStateDescriptor)
    }

    override def readKey(
                          key: String,
                          ctx: Context,
                          out: Collector[FlinkTuple2[String, String]]): Unit = {
      val data = new FlinkTuple2(key, valueState.value())
      out.collect(data)
    }
  }

  private def getParams(args: Array[String]): StateInteractorParams = {
    val savepointPath = Try(args(0)).toOption.getOrElse("C:/flink-data/savepoints/savepoint-342210-7879f8ccc844")
    val operatorUid = Try(args(1)).toOption.getOrElse("testOperator")
    val listStateName = Try(args(2)).toOption.getOrElse("testStringListState")
    val valueStateName = Try(args(3)).toOption.getOrElse("testValueState")
    val listStateCsvFilePath = Try(args(4)).toOption.getOrElse("file:///home/osboxes/flink-data/listState.csv")
    val valueStateCsvFilePath = Try(args(5)).toOption.getOrElse("file:///home/osboxes/flink-data/valueState.csv")
    val rocksDbStateBackendUri = Try(args(6)).toOption.getOrElse("file:///home/osboxes/flink-data/stateTestProjectReadState/")
    StateInteractorParams(savepointPath, operatorUid, listStateName, valueStateName, listStateCsvFilePath, valueStateCsvFilePath, rocksDbStateBackendUri)
  }
}

case class StateInteractorParams(savepointPath: String,
                                 operatorUid: String,
                                 listStateName: String,
                                 valueStateName: String,
                                 valueStateCsvFilePath: String,
                                 valueStateCsvFileName: String,
                                 rocksDbStateBackendUri: String)

package neoflex

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ValueState}
import org.apache.flink.api.common.typeinfo.{TypeInformation, Types}
import org.apache.flink.api.java.{DataSet, ExecutionEnvironment}
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.state.memory.MemoryStateBackend
import org.apache.flink.state.api.Savepoint
import org.apache.flink.state.api.functions.KeyedStateReaderFunction
import org.apache.flink.state.api.functions.KeyedStateReaderFunction.Context
import org.apache.flink.util.Collector

import scala.util.Try
import collection.JavaConverters._

object StateInteractor extends App {
  val params = getParams(args)
  val env = ExecutionEnvironment.getExecutionEnvironment
  val backEnd = new MemoryStateBackend()
  val savepoint = Savepoint.load(env, params.savepointPath, backEnd)
  //  val listState = savepoint.readListState(
  //    params.operatorUid,
  //    params.stateName,
  //    TypeInformation.of(classOf[String]))
  //  listState.print()
  //  println(savepoint)

  val keyedState = savepoint.readKeyedState(params.operatorUid, new ReaderFunction)
  keyedState.print()

  case class KeyedState(key: Int, valuesList: List[String])

  class ReaderFunction extends KeyedStateReaderFunction[Integer, KeyedState] {

    var listState: ListState[String] = _

    override def open(parameters: Configuration): Unit = {
      val listStateDescriptor = new ListStateDescriptor(params.stateName, Types.STRING)
      listState = getRuntimeContext.getListState(listStateDescriptor)
    }

    override def readKey(
                          key: Integer,
                          ctx: Context,
                          out: Collector[KeyedState]): Unit = {
      val data = KeyedState(key, listState.get.asScala.toList)
      out.collect(data)
    }
  }

  private def getParams(args: Array[String]): StateInteractorParams = {
    val savepointPath = Try(args(0)).toOption.getOrElse("C:/flink-data/savepoints/savepoint-07bcb8-4b2732f23cf6")
    val operatorUid = Try(args(1)).toOption.getOrElse("testOperator")
    val stateName = Try(args(2)).toOption.getOrElse("testStringListState")
    StateInteractorParams(savepointPath, operatorUid, stateName)
  }
}

case class StateInteractorParams(savepointPath: String, operatorUid: String, stateName: String)

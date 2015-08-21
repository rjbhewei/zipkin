package com.twitter.zipkin.storage.cassandra

import com.twitter.util._
import com.twitter.zipkin.common.Dependencies
import com.twitter.zipkin.conversions.thrift._
import com.twitter.zipkin.storage.DependencyStore
import com.twitter.zipkin.thriftscala.{Dependencies => ThriftDependencies}
import org.twitter.zipkin.storage.cassandra.Repository
import TimeConversions._
import scala.collection.JavaConverters._

class CassandraDependencyStore(repository: Repository) extends DependencyStore {
  private[this] val pool = FuturePool.unboundedPool
  private[this] val codec = new ScroogeThriftCodec[ThriftDependencies](ThriftDependencies)

  def close() = repository.close()

  override def getDependencies(
    startDate: Option[Time],
    endDate: Option[Time]): Future[Dependencies] = pool {
    val startTime = startDate.get // surely this is wrong
    val endTime = endDate.get // and surely this is wrong
    repository.getDependencies(startTime.inMillis, endTime.inMillis).asScala
        .map(codec.decode(_))
        .map(thriftToDependencies(_).toDependencies)
        .head // no doubt, this is wrong, too.
  }

  override def storeDependencies(dependencies: Dependencies): Future[Unit] = pool {
    val startFlooredToDay = dependencies.startTime.floor(1.day).inMillis
    val thrift = dependenciesToThrift(dependencies).toThrift
    repository.storeDependencies(startFlooredToDay, codec.encode(thrift))
  }
}

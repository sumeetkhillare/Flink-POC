/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dataartisans.queryablestatedemo;

import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.query.QueryableStateClient;
import org.apache.flink.runtime.query.netty.UnknownKeyOrNamespace;
import org.apache.flink.runtime.query.netty.message.KvStateRequestSerializer;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.runtime.util.DataInputDeserializer;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.FiniteDuration;
public class QueryClientHelper<K, V> implements AutoCloseable {
  private final JobID jobId;
  private final TypeSerializer<K> keySerializer;
  private final TypeSerializer<V> valueSerializer;
  private final FiniteDuration queryTimeout;
  private final QueryableStateClient client;

  QueryClientHelper(
      String jobManagerHost,
      int jobManagerPort,
      JobID jobId,
      TypeSerializer<K> keySerializer,
      TypeSerializer<V> valueSerializer,
      Time queryTimeout) throws Exception {

    this.jobId = jobId;
    this.keySerializer = keySerializer;
    this.valueSerializer = valueSerializer;
    this.queryTimeout = new FiniteDuration(queryTimeout.toMilliseconds(), TimeUnit.MILLISECONDS);

    Configuration config = new Configuration();
    config.setString(ConfigConstants.JOB_MANAGER_IPC_ADDRESS_KEY, jobManagerHost);
    config.setInteger(ConfigConstants.JOB_MANAGER_IPC_PORT_KEY, jobManagerPort);

    this.client = new QueryableStateClient(config);
  }
  Optional<V> queryState(String name, K key) throws Exception {
    if (name == null) {
      throw new NullPointerException("Name");
    }

    if (key == null) {
      throw new NullPointerException("Key");
    }
    byte[] serializedKey = KvStateRequestSerializer.serializeKeyAndNamespace(
        key,
        keySerializer,
        VoidNamespace.INSTANCE,
        VoidNamespaceSerializer.INSTANCE);

    Future<byte[]> queryFuture = client.getKvState(jobId, name, key.hashCode(), serializedKey);

    try {
      byte[] queryResult = Await.result(queryFuture, queryTimeout);

      DataInputDeserializer dis = new DataInputDeserializer(
          queryResult,
          0,
          queryResult.length);
      V value = valueSerializer.deserialize(dis);
      return Optional.ofNullable(value);
    } catch (UnknownKeyOrNamespace e) {
      return Optional.empty();
    }
  }

  @Override
  public void close() {
    client.shutDown();
  }

}

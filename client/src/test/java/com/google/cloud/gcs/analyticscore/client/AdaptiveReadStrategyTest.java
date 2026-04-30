/*
 * Copyright 2026 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.gcs.analyticscore.client;

import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.storage.Storage;
import com.google.cloud.storage.contrib.nio.testing.LocalStorageHelper;
import java.io.IOException;
import org.junit.jupiter.api.Test;

class AdaptiveReadStrategyTest {

  private final Storage storage = LocalStorageHelper.getOptions().getService();
  private final GcsItemId itemId =
      GcsItemId.builder().setBucketName("test-bucket").setObjectName("test-object").build();
  private final GcsReadOptions options =
      GcsReadOptions.builder()
          .setFileAccessPattern(FileAccessPattern.AUTO_SEQUENTIAL)
          .setInplaceSeekLimit(100)
          .build();
  private final GcsItemInfo itemInfo =
      GcsItemInfo.builder().setItemId(itemId).setSize(1000).setContentGeneration(0L).build();

  @Test
  void constructor_startsInSequentialMode() throws IOException {
    createBlobInStorage("a".repeat(1000));

    AdaptiveReadStrategy strategy = new AdaptiveReadStrategy(storage, itemId, options, itemInfo);

    assertThat(strategy.getLimit()).isEqualTo(Long.MAX_VALUE);
  }

  @Test
  void getReadChannel_smallForwardSeek_staysInSequentialMode() throws IOException {
    createBlobInStorage("a".repeat(1000));
    AdaptiveReadStrategy strategy = new AdaptiveReadStrategy(storage, itemId, options, itemInfo);

    strategy.getReadChannel(50, 10);

    assertThat(strategy.getLimit()).isEqualTo(Long.MAX_VALUE);
  }

  @Test
  void getReadChannel_largeForwardSeek_switchesToRandomMode() throws IOException {
    createBlobInStorage("a".repeat(1000));
    AdaptiveReadStrategy strategy = new AdaptiveReadStrategy(storage, itemId, options, itemInfo);

    strategy.getReadChannel(200, 10);

    assertThat(strategy.getLimit()).isEqualTo(210);
  }

  @Test
  void getReadChannel_backwardSeek_switchesToRandomMode() throws IOException {
    createBlobInStorage("a".repeat(1000));
    AdaptiveReadStrategy strategy = new AdaptiveReadStrategy(storage, itemId, options, itemInfo);
    strategy.getReadChannel(50, 10);
    strategy.position(60);

    strategy.getReadChannel(40, 10);

    assertThat(strategy.getLimit()).isEqualTo(50);
  }

  @Test
  void getReadChannel_afterSwitch_staysInRandomMode() throws IOException {
    createBlobInStorage("a".repeat(1000));
    AdaptiveReadStrategy strategy = new AdaptiveReadStrategy(storage, itemId, options, itemInfo);
    strategy.getReadChannel(200, 10);

    strategy.getReadChannel(210, 10);

    assertThat(strategy.getLimit()).isEqualTo(220);
  }

  @Test
  void constructor_withRandomPattern_startsInRandomMode() throws IOException {
    createBlobInStorage("a".repeat(1000));
    GcsReadOptions randomOptions =
        GcsReadOptions.builder().setFileAccessPattern(FileAccessPattern.RANDOM).build();

    AdaptiveReadStrategy strategy =
        new AdaptiveReadStrategy(storage, itemId, randomOptions, itemInfo);

    strategy.getReadChannel(0, 10);
    assertThat(strategy.getLimit()).isEqualTo(10);
  }

  @Test
  void getReadChannel_withSequentialPattern_doesNotSwitchOnLargeSeek() throws IOException {
    createBlobInStorage("a".repeat(1000));
    GcsReadOptions seqOptions =
        GcsReadOptions.builder()
            .setFileAccessPattern(FileAccessPattern.SEQUENTIAL)
            .setInplaceSeekLimit(100)
            .build();
    AdaptiveReadStrategy strategy = new AdaptiveReadStrategy(storage, itemId, seqOptions, itemInfo);

    strategy.getReadChannel(200, 10);

    assertThat(strategy.getLimit()).isEqualTo(Long.MAX_VALUE);
  }

  @Test
  void close_closesCurrentStrategy() throws IOException {
    AdaptiveReadStrategy adaptiveReadStrategy =
        new AdaptiveReadStrategy(storage, itemId, options, itemInfo);
    TrackingReadStrategy trackingReadStrategy = new TrackingReadStrategy(adaptiveReadStrategy);

    trackingReadStrategy.close();

    assertThat(trackingReadStrategy.getCloseCalls()).isEqualTo(1);
  }

  private void createBlobInStorage(String content) {
    StorageTestUtils.createBlobInStorage(storage, itemId, content);
  }
}

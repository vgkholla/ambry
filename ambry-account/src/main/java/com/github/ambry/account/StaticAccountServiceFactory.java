/*
 * Copyright 2017 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */
package com.github.ambry.account;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.config.StaticAccountServiceConfig;
import com.github.ambry.config.VerifiableProperties;


/**
 * Factory to help get an instance of {@link StaticAccountService}.
 */
public class StaticAccountServiceFactory implements AccountServiceFactory {
  private final StaticAccountServiceConfig config;
  private final AccountServiceMetrics metrics;

  /**
   * Constructor.
   * @param verifiableProperties The properties to get a {@link StaticAccountService} instance. Cannot be {@code null}.
   * @param metricRegistry The {@link MetricRegistry} for metrics tracking.
   * @param notifier ignored.
   */
  public StaticAccountServiceFactory(VerifiableProperties verifiableProperties, MetricRegistry metricRegistry,
      Object notifier) {
    config = new StaticAccountServiceConfig(verifiableProperties);
    metrics = new AccountServiceMetrics(metricRegistry);
  }

  @Override
  public AccountService getAccountService() throws InstantiationException {
    try {
      return new StaticAccountService(config, metrics);
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }
}

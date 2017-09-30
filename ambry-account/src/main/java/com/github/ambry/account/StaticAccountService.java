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

import com.github.ambry.config.StaticAccountServiceConfig;
import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.function.Consumer;
import org.json.JSONException;
import org.json.JSONObject;

import static com.github.ambry.utils.Utils.*;


class StaticAccountService implements AccountService {
  private static final String ACCOUNTS_FIELD = "Accounts";

  private final AccountInfoMap accountInfoMap;

  StaticAccountService(StaticAccountServiceConfig config, AccountServiceMetrics metrics)
      throws IOException, JSONException {
    File dataFile = new File(config.staticAccountServiceAccountsFilePath);
    if (!dataFile.exists() || !dataFile.isFile()) {
      throw new IllegalArgumentException(
          config.staticAccountServiceAccountsFilePath + " does not exist or is not a file");
    }
    JSONObject accountData =
        new JSONObject(readStringFromFile(config.staticAccountServiceAccountsFilePath)).getJSONObject(ACCOUNTS_FIELD);
    Map<String, String> accountsMap = new HashMap<>();
    Iterator keys = accountData.keys();
    while (keys.hasNext()) {
      String key = keys.next().toString();
      // ideally we want to just retain it as a JSON object because AccountInfoMap makes this a JSONObject again
      // but to avoid having another constructor that repeats code in AccountInfoMap, converting to String
      // Since this is only on startup, shouldn't make much of a difference.
      accountsMap.put(key, accountData.getString(key));
    }
    accountInfoMap = new AccountInfoMap(accountsMap, metrics);
  }

  @Override
  public Account getAccountById(short accountId) {
    return accountInfoMap.getAccountById(accountId);
  }

  @Override
  public Account getAccountByName(String accountName) {
    return accountInfoMap.getAccountByName(accountName);
  }

  @Override
  public boolean updateAccounts(Collection<Account> accounts) {
    throw new UnsupportedOperationException("The StaticAccountService cannot process updates");
  }

  @Override
  public Collection<Account> getAllAccounts() {
    return null;
  }

  @Override
  public boolean addAccountUpdateConsumer(Consumer<Collection<Account>> accountUpdateConsumer) {
    return true;
  }

  @Override
  public boolean removeAccountUpdateConsumer(Consumer<Collection<Account>> accountUpdateConsumer) {
    return true;
  }

  @Override
  public void close() throws IOException {
    // no op
  }
}

// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package com.starrocks.journal.kv;

import com.sleepycat.bind.tuple.TupleBinding;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.TransactionConfig;
import com.starrocks.journal.bdbje.BDBEnvironment;
import com.starrocks.journal.bdbje.CloseSafeDatabase;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.persist.metablock.SRMetaBlockID;
import com.starrocks.server.GlobalStateMgr;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

public class BDBKVClient {
    private BDBEnvironment bdbEnvironment = null;
    protected CloseSafeDatabase currentJournalDB = null;

    public BDBKVClient(BDBEnvironment bdbEnvironment) {
        this.bdbEnvironment = bdbEnvironment;
    }

    public void open() {
        currentJournalDB = bdbEnvironment.openDatabase("kv");
    }

    public void close() {
        currentJournalDB.close();
    }

    public Transaction startTransaction() {
        TransactionConfig transactionConfig = new TransactionConfig();
        transactionConfig.setSerializableIsolationVoid(true);

        return currentJournalDB.getDb().getEnvironment().beginTransaction(
                null, bdbEnvironment.getTxnConfig());
    }

    public DatabaseEntry get(Transaction transaction, DatabaseEntry key) {
        DatabaseEntry value = new DatabaseEntry();
        currentJournalDB.getDb().get(transaction, key, value, LockMode.READ_COMMITTED);
        return value;
    }

    public String get(Transaction transaction, String key) {
        TupleBinding<String> binding = TupleBinding.getPrimitiveBinding(String.class);
        DatabaseEntry keyEntry = new DatabaseEntry();
        binding.objectToEntry(key, keyEntry);

        DatabaseEntry valueEntry = GlobalStateMgr.getCurrentState().getKvJournal().get(transaction, keyEntry);
        if (valueEntry != null) {
        }

        return binding.entryToObject(valueEntry);
    }

    public Map<String, String> getPrefix(Transaction transaction, String key) {
        key.charAt(key.length() - 1);

        String value = key;
        char sets[] = value.toCharArray();
        sets[key.length() - 1] = sets[key.length() - 1];
        String v2 = new String(sets);

        Map<String, String> m = new HashMap<>();

        Cursor cursor = currentJournalDB.getDb().openCursor(transaction, null);

        TupleBinding<String> binding = TupleBinding.getPrimitiveBinding(String.class);
        DatabaseEntry keyEntry = new DatabaseEntry();
        binding.objectToEntry(key, keyEntry);

        DatabaseEntry valueEntry = new DatabaseEntry();
        cursor.getSearchKeyRange(keyEntry, valueEntry, LockMode.READ_COMMITTED);

        DatabaseEntry fKey2 = new DatabaseEntry();
        DatabaseEntry fValue2 = new DatabaseEntry();

        DatabaseEntry fKey = new DatabaseEntry();
        DatabaseEntry fValue = new DatabaseEntry();
        while (cursor.getNext(fKey, fValue, LockMode.READ_COMMITTED) == OperationStatus.SUCCESS) {
            String sKey = new String(fKey.getData(), StandardCharsets.UTF_8);
            String sValue = new String(fValue.getData(), StandardCharsets.UTF_8);

            if (sKey.compareTo(v2) < 0) {
                break;
            }
            m.put(sKey, sValue);
        }

        return m;
    }

    public boolean contains(Transaction transaction, Object object) {
        String key = GsonUtils.GSON.toJson(object);
        TupleBinding<String> binding = TupleBinding.getPrimitiveBinding(String.class);
        DatabaseEntry keyEntry = new DatabaseEntry();
        binding.objectToEntry(key, keyEntry);

        DatabaseEntry valueEntry = GlobalStateMgr.getCurrentState().getKvJournal().get(transaction, keyEntry);
        return valueEntry != null;
    }

    public OperationStatus putNoOverwrite(Transaction transaction, DatabaseEntry key, DatabaseEntry value) {
        return currentJournalDB.getDb().putNoOverwrite(transaction, key, value);
    }

    public OperationStatus putNoOverwrite(Transaction transaction, String prefix, Object key, Object value) {
        TupleBinding<String> binding = TupleBinding.getPrimitiveBinding(String.class);
        DatabaseEntry keyEntry = new DatabaseEntry();
        binding.objectToEntry(prefix + "/" + GsonUtils.GSON.toJson(key), keyEntry);
        DatabaseEntry valueEntry = new DatabaseEntry();
        binding.objectToEntry(GsonUtils.GSON.toJson(value), valueEntry);

        return putNoOverwrite(transaction, keyEntry, valueEntry);
    }

    public OperationStatus delete(Transaction transaction, String prefix, Object key) {
        TupleBinding<String> binding = TupleBinding.getPrimitiveBinding(String.class);
        DatabaseEntry keyEntry = new DatabaseEntry();
        binding.objectToEntry(prefix + "/" + GsonUtils.GSON.toJson(key), keyEntry);

        return currentJournalDB.getDb().delete(transaction, keyEntry);
    }

    public boolean isKVMeta(SRMetaBlockID srMetaBlockID) {
        //kv_meta/sr_meta_block_id
        Transaction transaction  = startTransaction();
        return true;
    }

    public void markTransferred(SRMetaBlockID srMetaBlockID) {
        Transaction transaction  = startTransaction();
        putNoOverwrite(transaction, "kv_meta/", String.valueOf(srMetaBlockID.getId()), null);
    }

    public void ignoreTransferred(SRMetaBlockID srMetaBlockID) {

    }
}


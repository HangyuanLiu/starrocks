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
package com.starrocks.common.util;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.sleepycat.je.Transaction;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.InternalCatalog;
import com.starrocks.common.DdlException;
import com.starrocks.common.Pair;
import com.starrocks.common.io.Writable;
import com.starrocks.journal.kv.BDBKVClient;
import com.starrocks.persist.OperationType;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.CreateFileStmt;
import com.starrocks.sql.ast.DropFileStmt;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class SmallFileMgrKV extends SmallFileMgr {

    //starrocks/time_travel/current_version
    //starrocks/time_travel/version/`version-id`

    //key: small_file/catalog_id/database_id/file_id/`id`
    //value: SmallFile

    //key: small_file/catalog_id/database_id/file_name/`name`
    //value: file_id

    /*
    * starrocks/small_file_mgr/catalog_id/database_id/file_name/version
    *   key: starrocks/small_file_mgr/file_id/`catalog_id`/`database_id`/`file_id`
    *   value: SmallFile
    *   eg: starrocks/small_file_mgr/file_name/10001/10002/10003
    *
    *   key: starrocks/small_file_mgr/file_name/`name`
     *  value: file_id
     *  eg: starrocks/small_file_mgr/file_name/kerberos-key
    * */
    static String prefix = "starrocks/small_file_mgr";

    private String buildPrefix(String catalogId, String databaseId, String suffix) {
        return Joiner.on("/").join(prefix, catalogId, databaseId, suffix);
    }

    @Override
    public void createFile(CreateFileStmt stmt) throws DdlException {
        String dbName = stmt.getDbName();
        Database db = GlobalStateMgr.getCurrentState().getDb(dbName);
        if (db == null) {
            throw new DdlException("Database " + dbName + " does not exist");
        }

        SmallFile smallFile = downloadAndCheck(db.getId(), stmt.getCatalogName(), stmt.getFileName(),
                stmt.getDownloadUrl(), stmt.getChecksum(), stmt.isSaveContent());

        BDBKVClient BDBKVClient = GlobalStateMgr.getCurrentState().getKvJournal();
        Transaction transaction = BDBKVClient.startTransaction();
        try {
            String fileName = stmt.getFileName();
            if (BDBKVClient.contains(transaction, prefix + "/" + fileName)) {
                throw new DdlException("File " + fileName + " already exist");
            }

            BDBKVClient.putNoOverwrite(transaction, prefix, smallFile.id, smallFile);
            BDBKVClient.putNoOverwrite(transaction, prefix, fileName, smallFile.id);

            List<Pair<Short, Writable>> opLogs = new ArrayList<>();
            opLogs.add(new Pair<>(OperationType.OP_CREATE_SMALL_FILE_V2, smallFile));
            boolean result = GlobalStateMgr.getCurrentState().getEditLog().commit(transaction, opLogs);
            if (!result) {
                throw new DdlException("");
            }
        } catch (Exception e) {
            transaction.abort();
        }
    }

    @Override
    public void dropFile(DropFileStmt stmt) throws DdlException {
        String dbName = stmt.getDbName();
        Database db = GlobalStateMgr.getCurrentState().getDb(dbName);
        if (db == null) {
            throw new DdlException("Database " + dbName + " does not exist");
        }

        String fileName = stmt.getFileName();

        BDBKVClient BDBKVClient = GlobalStateMgr.getCurrentState().getKvJournal();
        Transaction transaction = BDBKVClient.startTransaction();

        String smallFileJson = BDBKVClient.get(transaction, prefix + "/" + fileName);
        SmallFile smallFile = GsonUtils.GSON.fromJson(smallFileJson, SmallFile.class);

        BDBKVClient.delete(transaction, prefix, smallFile.id);
        BDBKVClient.delete(transaction, prefix, fileName);

        List<Pair<Short, Writable>> opLogs = new ArrayList<>();
        opLogs.add(new Pair<>(OperationType.OP_DROP_SMALL_FILE_V2, smallFile));
        boolean result = GlobalStateMgr.getCurrentState().getEditLog().commit(transaction, opLogs);
        if (!result) {
            throw new DdlException("");
        }
    }

    @Override
    public boolean containsFile(long dbId, String catalog, String fileName) {
        BDBKVClient BDBKVClient = GlobalStateMgr.getCurrentState().getKvJournal();
        Transaction transaction = BDBKVClient.startTransaction();
        boolean result = BDBKVClient.contains(transaction, prefix + "/" + fileName);

        transaction.commit();
        return result;
    }

    @Override
    public SmallFile getSmallFile(long dbId, String catalog, String fileName, boolean needContent)
            throws DdlException {
        BDBKVClient BDBKVClient = GlobalStateMgr.getCurrentState().getKvJournal();
        Transaction transaction = BDBKVClient.startTransaction();

        String smallFileJson = BDBKVClient.get(transaction, prefix + "/" + fileName);
        SmallFile smallFile = GsonUtils.GSON.fromJson(smallFileJson, SmallFile.class);

        return smallFile;
    }

    @Override
    public SmallFile getSmallFile(long fileId) {
        BDBKVClient BDBKVClient = GlobalStateMgr.getCurrentState().getKvJournal();
        Transaction transaction = BDBKVClient.startTransaction();

        String smallFileJson = BDBKVClient.get(transaction, prefix + "/" + fileId);
        SmallFile smallFile = GsonUtils.GSON.fromJson(smallFileJson, SmallFile.class);

        return smallFile;
    }

    @Override
    public List<List<String>> getInfo(String dbName) throws DdlException {
        Database db = GlobalStateMgr.getCurrentState().getDb(dbName);
        if (db == null) {
            throw new DdlException("Database " + dbName + " does not exist");
        }

        BDBKVClient BDBKVClient = GlobalStateMgr.getCurrentState().getKvJournal();
        Transaction transaction = BDBKVClient.startTransaction();

        Map<String, String> values = BDBKVClient.getPrefix(transaction, buildPrefix(
                String.valueOf(InternalCatalog.DEFAULT_INTERNAL_CATALOG_ID), String.valueOf(db.getId()), "file_id"));

        List<List<String>> infos = Lists.newArrayList();
        for (Map.Entry<String, String> file : values.entrySet()) {
            SmallFile smallFile = GsonUtils.GSON.fromJson(file.getValue(), SmallFile.class);
            List<String> info = Lists.newArrayList();

            info.add(String.valueOf(smallFile.id));
            info.add(dbName);
            info.add("catalog");
            info.add(smallFile.name);
            info.add(String.valueOf(smallFile.size)); // file size
            info.add(String.valueOf(smallFile.isContent));
            info.add(smallFile.md5);
            infos.add(info);
        }
        return infos;
    }

    public void upgradeToKV() {
        for (Long dbId : files.rowKeySet()) {
            Database db = GlobalStateMgr.getCurrentState().getDb(dbId);
            if (db == null) {
            }

            BDBKVClient BDBKVClient = GlobalStateMgr.getCurrentState().getKvJournal();
            Transaction transaction = BDBKVClient.startTransaction();

            Map<String, SmallFiles> dbFiles = files.row(db.getId());
            for (Map.Entry<String, SmallFiles> entry : dbFiles.entrySet()) {
                SmallFiles smallFiles = entry.getValue();
                for (Map.Entry<String, SmallFile> entry2 : smallFiles.getFiles().entrySet()) {
                    SmallFile smallFile = entry2.getValue();

                    BDBKVClient.putNoOverwrite(transaction, prefix, smallFile.id, smallFile);
                    BDBKVClient.putNoOverwrite(transaction, prefix, smallFile.name, smallFile.id);
                }
            }

            transaction.commit();
        }
    }
}

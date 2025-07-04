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

package org.apache.paimon.trino.catalog;

import org.apache.paimon.PagedList;
import org.apache.paimon.Snapshot;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.CatalogLoader;
import org.apache.paimon.catalog.Database;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.catalog.PropertyChange;
import org.apache.paimon.options.Options;
import org.apache.paimon.partition.Partition;
import org.apache.paimon.partition.PartitionStatistics;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.security.SecurityContext;
import org.apache.paimon.table.Instant;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.TableSnapshot;
import org.apache.paimon.trino.ClassLoaderUtils;
import org.apache.paimon.trino.fileio.TrinoFileIOLoader;

import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.spi.connector.ConnectorSession;
import org.apache.hadoop.conf.Configuration;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/** Trino catalog, use it after set session. */
public class TrinoCatalog implements Catalog {

    private final Options options;

    private final Configuration configuration;

    private final TrinoFileSystemFactory trinoFileSystemFactory;

    private Catalog current;

    private volatile boolean inited = false;

    public TrinoCatalog(
            Options options,
            Configuration configuration,
            TrinoFileSystemFactory trinoFileSystemFactory) {
        this.options = options;
        this.configuration = configuration;
        this.trinoFileSystemFactory = trinoFileSystemFactory;
    }

    public void initSession(ConnectorSession connectorSession) {
        if (!inited) {
            synchronized (this) {
                if (!inited) {
                    current =
                            ClassLoaderUtils.runWithContextClassLoader(
                                    () -> {
                                        TrinoFileSystem trinoFileSystem =
                                                trinoFileSystemFactory.create(connectorSession);
                                        CatalogContext catalogContext =
                                                CatalogContext.create(
                                                        options,
                                                        configuration,
                                                        new TrinoFileIOLoader(trinoFileSystem),
                                                        null);
                                        try {
                                            SecurityContext.install(catalogContext);
                                        } catch (Exception e) {
                                            throw new RuntimeException(e);
                                        }
                                        return CatalogFactory.createCatalog(catalogContext);
                                    },
                                    this.getClass().getClassLoader());
                    inited = true;
                }
            }
        }
    }

    @Override
    public Map<String, String> options() {
        if (!inited) {
            throw new RuntimeException("Not inited yet.");
        }
        return current.options();
    }

    @Override
    public List<String> listDatabases() {
        return current.listDatabases();
    }

    @Override
    public void createDatabase(String s, boolean b, Map<String, String> map)
            throws DatabaseAlreadyExistException {
        current.createDatabase(s, b, map);
    }

    @Override
    public Database getDatabase(String name) throws DatabaseNotExistException {
        return current.getDatabase(name);
    }

    @Override
    public void dropDatabase(String s, boolean b, boolean b1)
            throws DatabaseNotExistException, DatabaseNotEmptyException {
        current.dropDatabase(s, b, b1);
    }

    @Override
    public Table getTable(Identifier identifier) throws TableNotExistException {
        return current.getTable(identifier);
    }

    @Override
    public List<String> listTables(String s) throws DatabaseNotExistException {
        return current.listTables(s);
    }

    @Override
    public void dropTable(Identifier identifier, boolean b) throws TableNotExistException {
        current.dropTable(identifier, b);
    }

    @Override
    public void createTable(Identifier identifier, Schema schema, boolean ignoreIfExists)
            throws TableAlreadyExistException, DatabaseNotExistException {
        current.createTable(identifier, schema, ignoreIfExists);
    }

    @Override
    public void renameTable(Identifier fromTable, Identifier toTable, boolean ignoreIfExistsb)
            throws TableNotExistException, TableAlreadyExistException {
        current.renameTable(fromTable, toTable, ignoreIfExistsb);
    }

    @Override
    public void alterTable(Identifier identifier, List<SchemaChange> list, boolean ignoreIfExists)
            throws TableNotExistException, ColumnAlreadyExistException, ColumnNotExistException {
        current.alterTable(identifier, list, ignoreIfExists);
    }

    public List<Partition> listPartitions(Identifier identifier) throws TableNotExistException {
        return current.listPartitions(identifier);
    }

    @Override
    public void close() throws Exception {
        if (current != null) {
            current.close();
        }
    }

    @Override
    public void createDatabase(String name, boolean ignoreIfExists)
            throws DatabaseAlreadyExistException {
        current.createDatabase(name, ignoreIfExists);
    }

    @Override
    public void alterTable(Identifier identifier, SchemaChange change, boolean ignoreIfNotExists)
            throws TableNotExistException, ColumnAlreadyExistException, ColumnNotExistException {
        current.alterTable(identifier, change, ignoreIfNotExists);
    }

    @Override
    public void alterDatabase(String s, List<PropertyChange> list, boolean b)
            throws DatabaseNotExistException {}

    @Override
    public PagedList<String> listDatabasesPaged(@Nullable Integer integer, @Nullable String s) {
        return null;
    }

    @Override
    public PagedList<String> listTablesPaged(
            String s, @Nullable Integer integer, @Nullable String s1)
            throws DatabaseNotExistException {
        return null;
    }

    @Override
    public PagedList<Table> listTableDetailsPaged(
            String s, @Nullable Integer integer, @Nullable String s1)
            throws DatabaseNotExistException {
        return null;
    }

    @Override
    public void markDonePartitions(Identifier identifier, List<Map<String, String>> list)
            throws TableNotExistException {}

    @Override
    public PagedList<Partition> listPartitionsPaged(
            Identifier identifier, @Nullable Integer integer, @Nullable String s)
            throws TableNotExistException {
        return null;
    }

    @Override
    public boolean supportsListObjectsPaged() {
        return false;
    }

    @Override
    public boolean supportsVersionManagement() {
        return false;
    }

    @Override
    public boolean commitSnapshot(
            Identifier identifier, Snapshot snapshot, List<PartitionStatistics> list)
            throws TableNotExistException {
        return false;
    }

    @Override
    public Optional<TableSnapshot> loadSnapshot(Identifier identifier)
            throws TableNotExistException {
        return Optional.empty();
    }

    @Override
    public void rollbackTo(Identifier identifier, Instant instant) throws TableNotExistException {}

    @Override
    public void createBranch(Identifier identifier, String s, @Nullable String s1)
            throws TableNotExistException, BranchAlreadyExistException, TagNotExistException {}

    @Override
    public void dropBranch(Identifier identifier, String s) throws BranchNotExistException {}

    @Override
    public void fastForward(Identifier identifier, String s) throws BranchNotExistException {}

    @Override
    public List<String> listBranches(Identifier identifier) throws TableNotExistException {
        return List.of();
    }

    @Override
    public void createPartitions(Identifier identifier, List<Map<String, String>> list)
            throws TableNotExistException {}

    @Override
    public void dropPartitions(Identifier identifier, List<Map<String, String>> list)
            throws TableNotExistException {}

    @Override
    public void alterPartitions(Identifier identifier, List<PartitionStatistics> list)
            throws TableNotExistException {}

    @Override
    public CatalogLoader catalogLoader() {
        return null;
    }

    @Override
    public boolean caseSensitive() {
        return false;
    }
}

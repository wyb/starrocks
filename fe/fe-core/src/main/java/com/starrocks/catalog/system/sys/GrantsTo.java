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
package com.starrocks.catalog.system.sys;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.starrocks.authorization.ActionSet;
import com.starrocks.authorization.AuthorizationMgr;
import com.starrocks.authorization.CatalogPEntryObject;
import com.starrocks.authorization.DbPEntryObject;
import com.starrocks.authorization.FunctionPEntryObject;
import com.starrocks.authorization.ObjectType;
import com.starrocks.authorization.PipePEntryObject;
import com.starrocks.authorization.PrivilegeBuiltinConstants;
import com.starrocks.authorization.PrivilegeEntry;
import com.starrocks.authorization.PrivilegeType;
import com.starrocks.authorization.ResourceGroupPEntryObject;
import com.starrocks.authorization.ResourcePEntryObject;
import com.starrocks.authorization.StorageVolumePEntryObject;
import com.starrocks.authorization.TablePEntryObject;
import com.starrocks.authorization.UserPEntryObject;
import com.starrocks.authorization.WarehousePEntryObject;
import com.starrocks.catalog.Catalog;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.ExternalCatalog;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.GlobalFunctionMgr;
import com.starrocks.catalog.InternalCatalog;
import com.starrocks.catalog.ResourceGroup;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.system.SystemId;
import com.starrocks.catalog.system.SystemTable;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.CatalogMgr;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.MetadataMgr;
import com.starrocks.server.StorageVolumeMgr;
import com.starrocks.server.WarehouseManager;
import com.starrocks.sql.ast.UserIdentity;
import com.starrocks.thrift.TGetGrantsToRolesOrUserItem;
import com.starrocks.thrift.TGetGrantsToRolesOrUserRequest;
import com.starrocks.thrift.TGetGrantsToRolesOrUserResponse;
import com.starrocks.thrift.TGrantsToType;
import com.starrocks.thrift.TSchemaTableType;
import com.starrocks.warehouse.Warehouse;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.starrocks.catalog.system.SystemTable.NAME_CHAR_LEN;
import static com.starrocks.catalog.system.SystemTable.builder;

public class GrantsTo {
    private static final String GRANTS_TO_ROLES = "grants_to_roles";
    private static final String GRANTS_TO_USERS = "grants_to_users";

    public static SystemTable createGrantsToRoles() {
        return new SystemTable(SystemId.GRANTS_TO_ROLES_ID, GRANTS_TO_ROLES, Table.TableType.SCHEMA,
                builder()
                        .column("GRANTEE", ScalarType.createVarchar(NAME_CHAR_LEN))
                        .column("OBJECT_CATALOG", ScalarType.createVarchar(NAME_CHAR_LEN))
                        .column("OBJECT_DATABASE", ScalarType.createVarchar(NAME_CHAR_LEN))
                        .column("OBJECT_NAME", ScalarType.createVarchar(NAME_CHAR_LEN))
                        .column("OBJECT_TYPE", ScalarType.createVarchar(NAME_CHAR_LEN))
                        .column("PRIVILEGE_TYPE", ScalarType.createVarchar(NAME_CHAR_LEN))
                        .column("IS_GRANTABLE", ScalarType.createVarchar(NAME_CHAR_LEN))
                        .build(),
                TSchemaTableType.STARROCKS_GRANT_TO_ROLES);
    }

    public static SystemTable createGrantsToUsers() {
        return new SystemTable(SystemId.GRANTS_TO_USERS_ID, GRANTS_TO_USERS, Table.TableType.SCHEMA,
                builder()
                        .column("GRANTEE", ScalarType.createVarchar(NAME_CHAR_LEN))
                        .column("OBJECT_CATALOG", ScalarType.createVarchar(NAME_CHAR_LEN))
                        .column("OBJECT_DATABASE", ScalarType.createVarchar(NAME_CHAR_LEN))
                        .column("OBJECT_NAME", ScalarType.createVarchar(NAME_CHAR_LEN))
                        .column("OBJECT_TYPE", ScalarType.createVarchar(NAME_CHAR_LEN))
                        .column("PRIVILEGE_TYPE", ScalarType.createVarchar(NAME_CHAR_LEN))
                        .column("IS_GRANTABLE", ScalarType.createVarchar(NAME_CHAR_LEN))
                        .build(),
                TSchemaTableType.STARROCKS_GRANT_TO_USERS);
    }

    public static TGetGrantsToRolesOrUserResponse getGrantsTo(TGetGrantsToRolesOrUserRequest request) {
        AuthorizationMgr authorizationManager = GlobalStateMgr.getCurrentState().getAuthorizationMgr();
        TGetGrantsToRolesOrUserResponse tGetGrantsToRolesOrUserResponse = new TGetGrantsToRolesOrUserResponse();
        if (request.getType().equals(TGrantsToType.USER)) {
            Set<UserIdentity> userIdentities = authorizationManager.getAllUserIdentities();
            for (UserIdentity userIdentity : userIdentities) {
                if (userIdentity.equals(UserIdentity.ROOT)) {
                    continue;
                }

                Map<ObjectType, List<PrivilegeEntry>> privileges =
                        authorizationManager.getTypeToPrivilegeEntryListByUser(userIdentity);
                Set<TGetGrantsToRolesOrUserItem> items =
                        getGrantItems(authorizationManager, userIdentity.toString(), privileges);
                items.forEach(tGetGrantsToRolesOrUserResponse::addToGrants_to);
            }
        } else {
            List<String> roles = authorizationManager.getAllRoles();
            for (String grantee : roles) {
                if (PrivilegeBuiltinConstants.IMMUTABLE_BUILT_IN_ROLE_NAMES.contains(grantee)) {
                    continue;
                }

                Map<ObjectType, List<PrivilegeEntry>> privileges =
                        authorizationManager.getTypeToPrivilegeEntryListByRole(grantee);
                Set<TGetGrantsToRolesOrUserItem> items = getGrantItems(authorizationManager, grantee, privileges);
                items.forEach(tGetGrantsToRolesOrUserResponse::addToGrants_to);
            }
        }

        return tGetGrantsToRolesOrUserResponse;
    }

    private static Set<TGetGrantsToRolesOrUserItem> getGrantItems(
            AuthorizationMgr authorizationManager, String grantee,
            Map<ObjectType, List<PrivilegeEntry>> privileges) {

        ConnectContext context = new ConnectContext();

        MetadataMgr metadataMgr = GlobalStateMgr.getCurrentState().getMetadataMgr();
        Set<TGetGrantsToRolesOrUserItem> items = new HashSet<>();
        for (Map.Entry<ObjectType, List<PrivilegeEntry>> privEntry : privileges.entrySet()) {
            for (PrivilegeEntry privilegeEntry : privEntry.getValue()) {
                Set<List<String>> objects = new HashSet<>();
                if (ObjectType.CATALOG.equals(privEntry.getKey())) {
                    CatalogPEntryObject catalogPEntryObject = (CatalogPEntryObject) privilegeEntry.getObject();
                    if (catalogPEntryObject.getId() == PrivilegeBuiltinConstants.ALL_CATALOGS_ID) {
                        List<String> catalogs = new ArrayList<>();
                        catalogs.add(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME);

                        if (Config.enable_show_external_catalog_privilege) {
                            catalogs.addAll(GlobalStateMgr.getCurrentState().getCatalogMgr().getCatalogs().keySet()
                                    .stream().filter(catalogName ->
                                            !CatalogMgr.ResourceMappingCatalog.isResourceMappingCatalog(catalogName)
                                    ).collect(Collectors.toList()));
                        }

                        for (String catalogName : catalogs) {
                            objects.add(Lists.newArrayList(catalogName, null, null));
                        }
                    } else {
                        String catalogName = getCatalogName(catalogPEntryObject.getId());
                        if (catalogName == null) {
                            continue;
                        }
                        if (!catalogName.equals(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME)
                                && !Config.enable_show_external_catalog_privilege) {
                            continue;
                        }
                        objects.add(Lists.newArrayList(catalogName, null, null));
                    }
                } else if (ObjectType.DATABASE.equals(privEntry.getKey())) {
                    DbPEntryObject dbPEntryObject = (DbPEntryObject) privilegeEntry.getObject();
                    if (dbPEntryObject.getCatalogId() == PrivilegeBuiltinConstants.ALL_CATALOGS_ID) {
                        List<String> catalogs = new ArrayList<>();
                        catalogs.add(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME);
                        if (Config.enable_show_external_catalog_privilege) {
                            catalogs.addAll(GlobalStateMgr.getCurrentState().getCatalogMgr().getCatalogs().keySet()
                                    .stream().filter(catalogName ->
                                            !CatalogMgr.ResourceMappingCatalog.isResourceMappingCatalog(catalogName)
                                    ).collect(Collectors.toList()));
                        }

                        for (String catalogName : catalogs) {
                            objects.addAll(expandAllDatabases(context, metadataMgr, catalogName));
                        }
                    } else {
                        String catalogName = getCatalogName(dbPEntryObject.getCatalogId());
                        if (catalogName == null) {
                            continue;
                        }
                        if (!catalogName.equals(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME)
                                && !Config.enable_show_external_catalog_privilege) {
                            continue;
                        }

                        if (dbPEntryObject.getUUID().equalsIgnoreCase(PrivilegeBuiltinConstants.ALL_DATABASES_UUID)) {
                            objects.addAll(expandAllDatabases(context, metadataMgr, catalogName));
                        } else {
                            Database database;
                            if (CatalogMgr.isInternalCatalog(catalogName)) {
                                database = GlobalStateMgr.getCurrentState().getLocalMetastore()
                                        .getDb(Long.parseLong(dbPEntryObject.getUUID()));
                            } else {
                                String dbName = ExternalCatalog.getDbNameFromUUID(dbPEntryObject.getUUID());
                                database = metadataMgr.getDb(context, catalogName, dbName);
                            }
                            if (database == null) {
                                continue;
                            }
                            if (database.isSystemDatabase() || database.getFullName().equals("_statistics_")) {
                                continue;
                            }

                            objects.add(Lists.newArrayList(catalogName, database.getFullName(), null));
                        }
                    }
                } else if (ObjectType.TABLE.equals(privEntry.getKey())
                        || ObjectType.VIEW.equals(privEntry.getKey())
                        || ObjectType.MATERIALIZED_VIEW.equals(privEntry.getKey())) {
                    TablePEntryObject tablePEntryObject = (TablePEntryObject) privilegeEntry.getObject();
                    if (tablePEntryObject.getCatalogId() == PrivilegeBuiltinConstants.ALL_CATALOGS_ID) {
                        List<String> catalogs = new ArrayList<>();
                        catalogs.add(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME);
                        if (Config.enable_show_external_catalog_privilege) {
                            catalogs.addAll(GlobalStateMgr.getCurrentState().getCatalogMgr().getCatalogs().keySet()
                                    .stream().filter(catalogName ->
                                            !CatalogMgr.ResourceMappingCatalog.isResourceMappingCatalog(catalogName)
                                    ).collect(Collectors.toList()));
                        }

                        for (String catalogName : catalogs) {
                            objects.addAll(expandAllDatabaseAndTables(context, metadataMgr, catalogName, privEntry.getKey()));
                        }
                    } else {
                        String catalogName = getCatalogName(tablePEntryObject.getCatalogId());
                        if (catalogName == null) {
                            continue;
                        }
                        if (!catalogName.equals(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME)
                                && !Config.enable_show_external_catalog_privilege) {
                            continue;
                        }

                        if (tablePEntryObject.getDatabaseUUID().equalsIgnoreCase(
                                PrivilegeBuiltinConstants.ALL_DATABASES_UUID)) {
                            objects.addAll(expandAllDatabaseAndTables(context, metadataMgr, catalogName, privEntry.getKey()));
                        } else {
                            Database database;
                            if (CatalogMgr.isInternalCatalog(tablePEntryObject.getCatalogId())) {
                                database = GlobalStateMgr.getCurrentState().getLocalMetastore()
                                        .getDb(Long.parseLong(tablePEntryObject.getDatabaseUUID()));
                            } else {
                                String dbName = ExternalCatalog.getDbNameFromUUID(tablePEntryObject.getDatabaseUUID());
                                database = metadataMgr.getDb(context, catalogName, dbName);
                            }
                            if (database == null) {
                                continue;
                            }

                            if (database.isSystemDatabase() || database.getFullName().equals("_statistics_")) {
                                continue;
                            }

                            String dbName = database.getFullName();
                            if (tablePEntryObject.getTableUUID().equalsIgnoreCase(
                                    PrivilegeBuiltinConstants.ALL_TABLES_UUID)) {
                                objects.addAll(expandAllTables(context, metadataMgr, catalogName, dbName, privEntry.getKey()));
                            } else {
                                if (CatalogMgr.isInternalCatalog(tablePEntryObject.getCatalogId())) {
                                    Table table = GlobalStateMgr.getCurrentState().getLocalMetastore()
                                            .getTable(database.getId(), (Long.parseLong(tablePEntryObject.getTableUUID())));
                                    if (table == null) {
                                        continue;
                                    }
                                    objects.add(Lists.newArrayList(catalogName, dbName, table.getName()));
                                } else {
                                    String tableName = ExternalCatalog.getTableNameFromUUID(tablePEntryObject.getTableUUID());
                                    objects.add(Lists.newArrayList(catalogName, dbName, tableName));
                                }
                            }
                        }
                    }
                } else if (ObjectType.USER.equals(privEntry.getKey())) {
                    UserPEntryObject tablePEntryObject = (UserPEntryObject) privilegeEntry.getObject();
                    UserIdentity userIdentity = tablePEntryObject.getUserIdentity();
                    if (userIdentity == null) {
                        List<String> allUsers = authorizationManager.getAllUsers();
                        for (String user : allUsers) {
                            objects.add(Lists.newArrayList(null, null, user));
                        }
                    } else {
                        objects.add(Lists.newArrayList(null, null, userIdentity.toString()));
                    }
                } else if (ObjectType.RESOURCE.equals(privEntry.getKey())) {
                    ResourcePEntryObject resourcePEntryObject = (ResourcePEntryObject) privilegeEntry.getObject();
                    String resourceName = resourcePEntryObject.getName();
                    if (resourceName == null) {
                        Set<String> allResources = GlobalStateMgr.getCurrentState().getResourceMgr().getAllResourceName();
                        for (String resource : allResources) {
                            objects.add(Lists.newArrayList(null, null, resource));
                        }
                    } else {
                        objects.add(Lists.newArrayList(null, null, resourceName));
                    }
                } else if (ObjectType.RESOURCE_GROUP.equals(privEntry.getKey())) {
                    ResourceGroupPEntryObject resourceGroupPEntryObject =
                            (ResourceGroupPEntryObject) privilegeEntry.getObject();
                    long resourceGroupId = resourceGroupPEntryObject.getId();
                    if (resourceGroupId == PrivilegeBuiltinConstants.ALL_RESOURCE_GROUP_ID) {
                        Set<String> allResourceGroupNames = GlobalStateMgr.getCurrentState().getResourceGroupMgr()
                                .getAllResourceGroupNames();
                        for (String resource : allResourceGroupNames) {
                            objects.add(Lists.newArrayList(null, null, resource));
                        }
                    } else {
                        ResourceGroup resourceGroup =
                                GlobalStateMgr.getCurrentState().getResourceGroupMgr().getResourceGroup(resourceGroupId);
                        if (resourceGroup == null) {
                            continue;
                        }
                        objects.add(Lists.newArrayList(null, null, resourceGroup.getName()));
                    }
                } else if (ObjectType.FUNCTION.equals(privEntry.getKey())) {
                    FunctionPEntryObject functionPEntryObject = (FunctionPEntryObject) privilegeEntry.getObject();
                    long databaseId = functionPEntryObject.getDatabaseId();
                    if (databaseId == PrivilegeBuiltinConstants.ALL_DATABASE_ID) {
                        List<String> dbNames = metadataMgr.listDbNames(context, InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME);
                        for (String dbName : dbNames) {
                            Database database = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbName);
                            if (database == null) {
                                continue;
                            }
                            if (database.isSystemDatabase() || database.getFullName().equals("_statistics_")) {
                                continue;
                            }
                            List<Function> functions = database.getFunctions();
                            for (Function function : functions) {
                                objects.add(Lists.newArrayList(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                                        database.getFullName(), function.signatureString()));
                            }
                        }
                    } else {
                        Database database = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(databaseId);
                        if (database == null) {
                            continue;
                        }
                        if (functionPEntryObject.getFunctionId().equals(PrivilegeBuiltinConstants.ALL_FUNCTIONS_ID)) {
                            List<Function> functions = database.getFunctions();
                            for (Function function : functions) {
                                objects.add(Lists.newArrayList(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                                        database.getFullName(), function.signatureString()));
                            }
                        } else {
                            for (Function f : database.getFunctions()) {
                                if (f.getFunctionId() == functionPEntryObject.getFunctionId()) {
                                    objects.add(Lists.newArrayList(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                                            database.getFullName(), f.signatureString()));
                                    break;
                                }
                            }
                        }
                    }
                } else if (ObjectType.GLOBAL_FUNCTION.equals(privEntry.getKey())) {
                    FunctionPEntryObject globalFunctionPEntryObject =
                            (FunctionPEntryObject) privilegeEntry.getObject();
                    GlobalFunctionMgr globalFunctionMgr = GlobalStateMgr.getCurrentState().getGlobalFunctionMgr();

                    if (globalFunctionPEntryObject.getFunctionId() == PrivilegeBuiltinConstants.ALL_FUNCTIONS_ID) {
                        for (Function function : globalFunctionMgr.getFunctions()) {
                            objects.add(Lists.newArrayList(null, null, function.signatureString()));
                        }
                    } else {
                        for (Function f : globalFunctionMgr.getFunctions()) {
                            if (f.getFunctionId() == globalFunctionPEntryObject.getFunctionId()) {
                                objects.add(Lists.newArrayList(null, null, f.signatureString()));
                                break;
                            }
                        }
                    }
                } else if (ObjectType.SYSTEM.equals(privEntry.getKey())) {
                    objects.add(Lists.newArrayList(null, null, null));
                } else if (ObjectType.STORAGE_VOLUME.equals(privEntry.getKey())) {
                    StorageVolumePEntryObject storageVolumePEntryObject =
                            (StorageVolumePEntryObject) privilegeEntry.getObject();
                    String storageVolumeId = storageVolumePEntryObject.getId();
                    StorageVolumeMgr storageVolumeMgr = GlobalStateMgr.getCurrentState().getStorageVolumeMgr();
                    if (storageVolumeId.equals(PrivilegeBuiltinConstants.ALL_STORAGE_VOLUMES_ID)) {
                        try {
                            List<String> storageVolumeNames = storageVolumeMgr.listStorageVolumeNames();
                            for (String storageVolumeName : storageVolumeNames) {
                                objects.add(Lists.newArrayList(null, null, storageVolumeName));
                            }
                        } catch (DdlException e) {
                            continue;
                        }
                    } else {
                        String storageVolumeName = storageVolumeMgr.getStorageVolumeName(storageVolumeId);
                        objects.add(Lists.newArrayList(null, null, storageVolumeName));
                    }

                } else if (ObjectType.PIPE.equals(privEntry.getKey())) {
                    PipePEntryObject pipePEntryObject = (PipePEntryObject) privilegeEntry.getObject();
                    objects.addAll(pipePEntryObject.expandObjectNames());
                } else if (ObjectType.WAREHOUSE.equals(privEntry.getKey())) {
                    WarehousePEntryObject warehousePEntryObject =
                            (WarehousePEntryObject) privilegeEntry.getObject();
                    long warehouseId = warehousePEntryObject.getId();
                    if (warehouseId == PrivilegeBuiltinConstants.ALL_WAREHOUSES_ID) {
                        WarehouseManager warehouseManager = GlobalStateMgr.getCurrentState().getWarehouseMgr();
                        Set<String> allWarehouseNames = warehouseManager.getAllWarehouseNames();
                        for (String warehouseName : allWarehouseNames) {
                            objects.add(Lists.newArrayList(null, null, warehouseName));
                        }
                    } else {
                        Warehouse warehouse =
                                GlobalStateMgr.getCurrentState().getWarehouseMgr().getWarehouseAllowNull(warehouseId);
                        if (warehouse == null) {
                            continue;
                        }
                        objects.add(Lists.newArrayList(null, null, warehouse.getName()));
                    }
                }

                ActionSet actionSet = privilegeEntry.getActionSet();
                List<PrivilegeType> privilegeTypes = authorizationManager.analyzeActionSet(privEntry.getKey(), actionSet);

                for (List<String> object : objects) {
                    TGetGrantsToRolesOrUserItem tGetGrantsToRolesOrUserItem = new TGetGrantsToRolesOrUserItem();
                    tGetGrantsToRolesOrUserItem.setGrantee(grantee);
                    tGetGrantsToRolesOrUserItem.setObject_catalog(object.get(0));
                    tGetGrantsToRolesOrUserItem.setObject_database(object.get(1));
                    tGetGrantsToRolesOrUserItem.setObject_name(object.get(2));
                    tGetGrantsToRolesOrUserItem.setObject_type(privEntry.getKey().name().replace("_", " "));
                    tGetGrantsToRolesOrUserItem.setPrivilege_type(Joiner.on(", ").join(privilegeTypes.stream().map(
                            privilegeType -> privilegeType.name().replace("_", " ")).collect(Collectors.toList())));
                    tGetGrantsToRolesOrUserItem.setIs_grantable(privilegeEntry.isWithGrantOption());

                    items.add(tGetGrantsToRolesOrUserItem);
                }
            }
        }
        return items;
    }

    private static String getCatalogName(Long catalogId) {
        String catalogName;
        if (catalogId == InternalCatalog.DEFAULT_INTERNAL_CATALOG_ID) {
            catalogName = InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME;
        } else {
            Optional<Catalog> catalogOptional = GlobalStateMgr.getCurrentState().getCatalogMgr().getCatalogById(catalogId);
            if (!catalogOptional.isPresent()) {
                return null;
            }
            Catalog catalog = catalogOptional.get();
            catalogName = catalog.getName();
        }
        return catalogName;
    }

    private static Set<List<String>> expandAllDatabases(ConnectContext context, MetadataMgr metadataMgr, String catalogName) {
        Set<List<String>> objects = new HashSet<>();
        if (!catalogName.equals(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME)
                && !Config.enable_show_external_catalog_privilege) {
            return objects;
        }

        List<String> dbNames = metadataMgr.listDbNames(context, catalogName);
        for (String dbName : dbNames) {
            Database database = metadataMgr.getDb(context, catalogName, dbName);
            if (database == null) {
                continue;
            }
            if (database.isSystemDatabase() || database.getFullName().equals("_statistics_")) {
                continue;
            }
            objects.add(Lists.newArrayList(catalogName, database.getFullName(), null));
        }
        return objects;
    }

    private static Set<List<String>> expandAllTables(ConnectContext context, MetadataMgr metadataMgr, String catalogName,
                                                     String dbName,
                                                     ObjectType objectType) {
        Set<List<String>> objects = new HashSet<>();

        List<String> tableNames = metadataMgr.listTableNames(context, catalogName, dbName);
        if (CatalogMgr.isInternalCatalog(catalogName)) {
            for (String tableName : tableNames) {
                Table table = metadataMgr.getTable(context, catalogName, dbName, tableName);
                if (table == null) {
                    continue;
                }

                if (objectType.equals(ObjectType.VIEW)) {
                    if (table.isOlapView()) {
                        objects.add(Lists.newArrayList(catalogName, dbName, table.getName()));
                    }
                } else if (objectType.equals(ObjectType.MATERIALIZED_VIEW)) {
                    if (table.isMaterializedView()) {
                        objects.add(Lists.newArrayList(catalogName, dbName, table.getName()));
                    }
                } else {
                    if (!table.isOlapView() && !table.isMaterializedView()) {
                        objects.add(Lists.newArrayList(catalogName, dbName, table.getName()));
                    }
                }
            }
        } else {
            if (Config.enable_show_external_catalog_privilege) {
                for (String tableName : tableNames) {
                    objects.add(Lists.newArrayList(catalogName, dbName, tableName));
                }
            }
        }
        return objects;
    }

    private static Set<List<String>> expandAllDatabaseAndTables(ConnectContext context, MetadataMgr metadataMgr,
                                                                String catalogName,
                                                                ObjectType objectType) {
        Set<List<String>> objects = new HashSet<>();
        if (!catalogName.equals(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME)
                && !Config.enable_show_external_catalog_privilege) {
            return objects;
        }

        List<String> dbNames = metadataMgr.listDbNames(context, catalogName);
        for (String dbName : dbNames) {
            Database database = metadataMgr.getDb(context, catalogName, dbName);
            if (database == null) {
                continue;
            }
            if (database.isSystemDatabase() || database.getFullName().equals("_statistics_")) {
                continue;
            }

            objects.addAll(expandAllTables(context, metadataMgr, catalogName, dbName, objectType));
        }

        return objects;
    }
}
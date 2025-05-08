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

package com.starrocks.fs;

import com.google.common.base.Strings;
import com.starrocks.common.Config;
import com.starrocks.common.StarRocksException;
import com.starrocks.fs.azure.AzBlobFileSystem;
import com.starrocks.fs.hdfs.HdfsFileSystemWrap;
import com.starrocks.fs.hdfs.HdfsFsManager;
import com.starrocks.fs.hdfs.WildcardURI;
import com.starrocks.thrift.THdfsProperties;
import org.apache.hadoop.fs.FileStatus;

import java.util.List;
import java.util.Map;

public interface FileSystem {

    public static FileSystem getFileSystem(String path, Map<String, String> properties) throws StarRocksException {
        WildcardURI pathUri = new WildcardURI(path);
        String scheme = pathUri.getUri().getScheme();
        if (Strings.isNullOrEmpty(scheme)) {
            throw new StarRocksException("invalid path. scheme is null");
        }

        if (Config.azure_use_native_sdk &&
                (scheme.equalsIgnoreCase(HdfsFsManager.WASB_SCHEME) || scheme.equalsIgnoreCase(HdfsFsManager.WASBS_SCHEME))) {
            return new AzBlobFileSystem(properties);
        } else {
            return new HdfsFileSystemWrap(properties);
        }
    }

    public List<FileStatus> globList(String path, boolean skipDir) throws StarRocksException;

    public THdfsProperties getHdfsProperties(String path) throws StarRocksException;
}

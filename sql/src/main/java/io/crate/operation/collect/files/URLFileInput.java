/*
 * Licensed to Crate.io Inc. (Crate) under one or more contributor license
 * agreements.  See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file to
 * you under the Apache License, Version 2.0 (the "License");  you may not
 * use this file except in compliance with the License.  You may obtain a
 * copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, to use any modules in this file marked as "Enterprise Features",
 * Crate must have given you permission to enable and use such Enterprise
 * Features and you must have a valid Enterprise or Subscription Agreement
 * with Crate.  If you enable or use the Enterprise Features, you represent
 * and warrant that you have a valid Enterprise or Subscription Agreement
 * with Crate.  Your use of the Enterprise Features if governed by the terms
 * and conditions of your Enterprise or Subscription Agreement with Crate.
 */

package io.crate.operation.collect.files;

import com.google.common.base.Predicate;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URL;
import java.util.Collections;
import java.util.List;

class URLFileInput implements FileInput {

    private final URI fileUri;

    public URLFileInput(URI fileUri) {
        this.fileUri = fileUri;
    }

    @Override
    public List<URI> listUris(URI fileUri, Predicate<URI> uriPredicate) throws IOException {
        // If the full fileUri contains a wildcard the fileUri passed as argument here is the fileUri up to the wildcard
        // for URLs listing directory contents is not supported so always return the full fileUri for now
        return Collections.singletonList(this.fileUri);
    }

    @Override
    public InputStream getStream(URI uri) throws IOException {
        URL url = uri.toURL();
        try {
            return url.openStream();
        } catch (FileNotFoundException e) {
            return null;
        }
    }

    @Override
    public boolean sharedStorageDefault() {
        return true;
    }
}

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

package io.crate.rest.action.admin;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.*;

import static org.elasticsearch.rest.RestRequest.Method.GET;

/**
 * RestHandlerAction to return a html page containing informations about crate
 */
public class AdminUIFrontpageAction extends BaseRestHandler {

    private final RestController controller;
    private final AdminUIStaticFileRequestFilter requestFilter;

    @Inject
    public AdminUIFrontpageAction(Settings settings, Client client, RestController controller, AdminUIStaticFileRequestFilter staticFileRequestFilter) {
        super(settings, controller, client);
        this.controller = controller;
        this.requestFilter = staticFileRequestFilter;
    }

    public void registerHandler() {
        controller.registerHandler(GET, "/admin", this);
        controller.registerFilter(requestFilter);
    }

    @Override
    protected void handleRequest(RestRequest request, RestChannel channel, Client client) throws Exception {
        BytesRestResponse resp = new BytesRestResponse(RestStatus.TEMPORARY_REDIRECT);
        resp.addHeader("Location", "/");
        channel.sendResponse(resp);
    }

}

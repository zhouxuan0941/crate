/*
 * Licensed to Crate.IO GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.operation.reference.sys.check.cluster;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import io.crate.metadata.ClusterReferenceResolver;
import io.crate.metadata.Reference;
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.settings.CrateSettings;
import io.crate.metadata.sys.SysClusterTableInfo;
import io.crate.operation.reference.sys.check.AbstractSysCheck;
import io.crate.operation.reference.sys.cluster.ClusterSettingsExpression;
import io.crate.types.DataTypes;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;

@Singleton
public class LicenseEnterpriseChecks extends AbstractSysCheck {

    private final ClusterService clusterService;
    private final ClusterReferenceResolver clusterReferenceResolver;

    private static final int ID = 5;
    private static final String DESCRIPTION = "You are currently using the Enterprise Edition, " +
    "but have not configured a licence. Please configure a license or deactivate the Enterprise Edition.";

    private static final Reference LICENCE_ENTERPRISE_INFO = new Reference(
        new ReferenceIdent(
            SysClusterTableInfo.IDENT,
            ClusterSettingsExpression.NAME,
            Lists.newArrayList(Splitter.on(".")
                .split(CrateSettings.LICENSE_ENTERPRISE.settingName()))
        ),
        RowGranularity.DOC, DataTypes.BOOLEAN
    );

    private static final Reference LICENCE_IDENT_INFO = new Reference(
        new ReferenceIdent(
            SysClusterTableInfo.IDENT,
            ClusterSettingsExpression.NAME,
            Lists.newArrayList(Splitter.on(".")
                .split(CrateSettings.LICENSE_IDENT.settingName()))
        ),
        RowGranularity.DOC, DataTypes.STRING
    );

    @Inject
    public LicenseEnterpriseChecks(ClusterService clusterService, ClusterReferenceResolver clusterReferenceResolver) {
        super(ID, DESCRIPTION, Severity.HIGH);
        this.clusterService = clusterService;
        this.clusterReferenceResolver = clusterReferenceResolver;
    }

    @Override
    public boolean validate() {
        return validate((Boolean) clusterReferenceResolver.getImplementation(LICENCE_ENTERPRISE_INFO).value(),
            clusterReferenceResolver.getImplementation(LICENCE_IDENT_INFO).value().toString());
    }

    protected boolean validate(boolean licenceEnterprise, String licenceIdent) {
        return licenceEnterprise && licenceIdent.equals("not present");
    }

}

package io.crate.protocols.postgres;


import com.google.common.base.Splitter;
import io.crate.ClusterIdService;
import io.crate.settings.CrateSetting;
import io.crate.types.DataTypes;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Provider;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static java.util.Collections.emptyList;
import static org.elasticsearch.common.settings.Setting.listSetting;

public class AuthenticationService extends AbstractLifecycleComponent {

    private final ClusterService clusterService;
    private final Provider<ClusterIdService> clusterIdServiceProvider;

    static Function<String, XContentBuilder> parseSettings =
        t -> {
            try {
                return XContentFactory.jsonBuilder().value(t);
            }
            catch(IOException e) {
                throw new UncheckedIOException(e);
            }
        };

    public static final Setting<List<XContentBuilder>> DEFAULT =
        listSetting("", emptyList(), parseSettings);

    public static final CrateSetting<List<XContentBuilder>> SETTING_AUTH_HBA = CrateSetting.of(
        listSetting("auth.host_based", DEFAULT, parseSettings, Setting.Property.NodeScope));


    @Inject
    public AuthenticationService(Settings settings,
                      ClusterService clusterService,
                      Provider<ClusterIdService> clusterIdServiceProvider) {
        super(settings);
        this.clusterService = clusterService;
        this.clusterIdServiceProvider = clusterIdServiceProvider;
    }
    @Override
    protected void doStart() throws ElasticsearchException {
    }

    @Override
    protected void doStop() throws ElasticsearchException {
    }

    @Override
    protected void doClose() throws ElasticsearchException {
    }
}

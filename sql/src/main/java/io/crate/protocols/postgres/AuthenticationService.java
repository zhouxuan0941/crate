package io.crate.protocols.postgres;


import io.crate.ClusterIdService;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Provider;
import org.elasticsearch.common.settings.Settings;

public class AuthenticationService extends AbstractLifecycleComponent {

    private final ClusterService clusterService;
    private final Provider<ClusterIdService> clusterIdServiceProvider;

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

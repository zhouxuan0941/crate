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

import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static java.util.Collections.emptyList;
import static org.elasticsearch.common.settings.Setting.listSetting;

public class AuthenticationService extends AbstractLifecycleComponent {

    private final ClusterService clusterService;
    private final Provider<ClusterIdService> clusterIdServiceProvider;

    static Function<String, Object> parseSettings = t -> Splitter.on(",").withKeyValueSeparator(":").split(t);

    public static final Setting<List<Object>> DEFAULT =
        listSetting("", emptyList(), parseSettings, Setting.Property.NodeScope);

    public static final CrateSetting<List<Object>> SETTING_AUTH_HBA = CrateSetting.of(
        listSetting("auth.host_based", DEFAULT, parseSettings, Setting.Property.NodeScope));



    public class HBAEntry {
        private String username;
        private String method;
        private String address;

        public String getUsername() {
            return username;
        }

        public String getMethod() {
            return method;
        }

        public String getAddress() {
            return address;
        }

        public void setUsername(String username) {
            this.username = username;
        }

        public void setMethod(String method) {
            this.method = method;
        }

        public void setAddress(String address) {
            this.address = address;
        }

        @Override
        public String toString() {
            return "HBAEntry [user=" + username + ", method=" + method + ", " +
                "address=" + address + "]";
        }

    }
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

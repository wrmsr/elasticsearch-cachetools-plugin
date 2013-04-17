package com.yelp.elasticsearch.plugins.cachetools;

import com.google.common.collect.ImmutableList;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.inject.Scopes;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.cache.IndexCache;
import org.elasticsearch.plugins.AbstractPlugin;
import org.elasticsearch.rest.*;
import org.elasticsearch.rest.action.support.RestXContentBuilder;

import java.io.IOException;
import java.util.Collection;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestStatus.OK;

public class CacheToolsPlugin extends AbstractPlugin {

    private Settings settings;

    @Inject
    public CacheToolsPlugin(Settings settings) {
        this.settings = settings;
    }

    @Override
    public String name() {
        return "cachetools";
    }

    @Override
    public String description() {
        return "Some plugin";
    }

    @Override
    public Collection<Module> modules(Settings settings) {
        return ImmutableList.of((Module) new CacheContentsActionModule());
    }

    public static class CacheContentsActionModule extends AbstractModule {

        @Override
        protected void configure() {
            bind(CacheContentsAction.class).in(Scopes.SINGLETON);
        }
    }

    public static class CacheContentsAction extends BaseRestHandler {

        private IndexCache indexCache;

        @Inject
        public CacheContentsAction(Settings settings, Client client, RestController controller) {
            super(settings, client);
            controller.registerHandler(GET, "/_cache/contents", this);
        }

        @Override
        public void handleRequest(final RestRequest request, final RestChannel channel) {
            try {
                XContentBuilder builder = RestXContentBuilder.restContentBuilder(request);
                builder.startObject();
                builder.field("has_cache", true);
                builder.endObject();
                channel.sendResponse(new XContentRestResponse(request, OK, builder));
            } catch (IOException ioex) {
                try {
                    ioex.printStackTrace();
                    logger.error("CacheContents failure", ioex);
                    channel.sendResponse(new XContentThrowableRestResponse(request, ioex));
                } catch (IOException ioex1) {
                    logger.error("CacheContents double failure", ioex);
                }
            }
        }
    }
}

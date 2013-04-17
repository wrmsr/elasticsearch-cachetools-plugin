package com.yelp.elasticsearch.plugins.cachetools;

import org.elasticsearch.action.*;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.internal.InternalGenericClient;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.lucene.docset.DocSet;
import org.elasticsearch.common.lucene.docset.FixedBitDocSet;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.cache.filter.weighted.WeightedFilterCache;
import org.elasticsearch.indices.cache.filter.IndicesFilterCache;
import org.elasticsearch.rest.*;
import org.elasticsearch.rest.action.support.RestXContentBuilder;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestStatus.OK;

public class GetCacheContents {

    public static void processModule(Module module) {
        if (module instanceof ActionModule) {
            ActionModule actionModule = (ActionModule) module;
            actionModule.registerAction(GetCacheContentsAction.INSTANCE, TransportGetCacheContentsAction.class);
        } else if (module instanceof RestModule) {
            RestModule restModule = (RestModule) module;
            restModule.addRestAction(RestGetCacheContentsAction.class);
        }
    }

    public static class GetCacheContentsAction extends
            Action<GetCacheContentsRequest, GetCacheContentsResponse, GetCacheContentsRequestBuilder> {

        public static final GetCacheContentsAction INSTANCE = new GetCacheContentsAction();
        public static final String NAME = "getCacheContents";

        private GetCacheContentsAction() {
            super(NAME);
        }

        public GetCacheContentsResponse newResponse() {
            return new GetCacheContentsResponse();
        }

        public GetCacheContentsRequestBuilder newRequestBuilder(Client client) {
            return new GetCacheContentsRequestBuilder(client);
        }
    }

    public static class GetCacheContentsRequest extends ActionRequest<GetCacheContentsRequest> {

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }
    }

    public static class GetCacheContentsResponse extends ActionResponse {

        private ArrayList<Map<String, String>> cacheEntries;

        public GetCacheContentsResponse() {
            cacheEntries = new ArrayList<Map<String, String>>();
        }

        public void addCacheEntry(Map<String, String> cacheEntry) {
            cacheEntries.add(cacheEntry);
        }

        public List<Map<String, String>> getCacheEntries() {
            return cacheEntries;
        }
    }

    public static class GetCacheContentsRequestBuilder extends
            ActionRequestBuilder<GetCacheContentsRequest, GetCacheContentsResponse, GetCacheContentsRequestBuilder> {

        private final GetCacheContentsClientWrapper getCacheContentsClientWrapper;

        public GetCacheContentsRequestBuilder(Client client) {
            super((InternalGenericClient) client, new GetCacheContentsRequest());
            getCacheContentsClientWrapper = new GetCacheContentsClientWrapper(client);
        }

        @Override
        protected void doExecute(ActionListener<GetCacheContentsResponse> listener) {
            getCacheContentsClientWrapper.getCacheContents(request, listener);
        }
    }

    public static class TransportGetCacheContentsAction extends
            TransportAction<GetCacheContentsRequest, GetCacheContentsResponse> {

        private IndicesFilterCache indicesFilterCache;

        @Inject
        public TransportGetCacheContentsAction(Settings settings, ThreadPool threadPool,
                                               IndicesFilterCache indicesFilterCache) {
            super(settings, threadPool);
            this.indicesFilterCache = indicesFilterCache;
        }

        protected void doExecute(GetCacheContentsRequest request, ActionListener<GetCacheContentsResponse> listener) {
            GetCacheContentsResponse response = new GetCacheContentsResponse();
            ConcurrentMap<WeightedFilterCache.FilterCacheKey, DocSet> filterCacheMap =
                    indicesFilterCache.cache().asMap();
            for (Map.Entry<WeightedFilterCache.FilterCacheKey, DocSet> cacheEntry : filterCacheMap.entrySet()) {
                WeightedFilterCache.FilterCacheKey cacheKey = cacheEntry.getKey();
                Map<String, String> responseEntry = new HashMap<String, String>();

                responseEntry.put("reader_key", cacheKey.readerKey().toString());
                responseEntry.put("filter_key", cacheKey.filterKey().toString());
                DocSet docSet = cacheEntry.getValue();
                responseEntry.put("length", String.valueOf(docSet.length()));
                responseEntry.put("size_in_bytes", String.valueOf(docSet.sizeInBytes()));
                responseEntry.put("doc_set_class", docSet.getClass().getName());
                if (docSet instanceof FixedBitDocSet) {
                    FixedBitDocSet bitDocSet = (FixedBitDocSet) cacheEntry.getValue();
                    responseEntry.put("cardinality", String.valueOf(bitDocSet.set().cardinality()));
                }

                response.addCacheEntry(responseEntry);
            }
            listener.onResponse(response);
        }
    }

    public static class GetCacheContentsClientWrapper {

        protected final Client client;

        public GetCacheContentsClientWrapper(Client client) {
            this.client = client;
        }

        public void getCacheContents(GetCacheContentsRequest request, ActionListener<GetCacheContentsResponse> listener) {
            client.execute(GetCacheContentsAction.INSTANCE, request, listener);
        }

        public ActionFuture<GetCacheContentsResponse> getCacheContents(GetCacheContentsRequest request) {
            return client.execute(GetCacheContentsAction.INSTANCE, request);
        }

        public GetCacheContentsRequestBuilder prepareGetCacheContents() {
            return new GetCacheContentsRequestBuilder(client);
        }
    }

    public static class RestGetCacheContentsAction extends BaseRestHandler {

        @Inject
        public RestGetCacheContentsAction(Settings settings, Client client, RestController controller) {
            super(settings, client);
            controller.registerHandler(GET, "/_cache/contents", this);
        }

        @Override
        public void handleRequest(final RestRequest request, final RestChannel channel) {
            GetCacheContentsClientWrapper client =
                    new GetCacheContentsClientWrapper(this.client);

            client.prepareGetCacheContents().execute(new ActionListener<GetCacheContentsResponse>() {

                @Override
                public void onResponse(GetCacheContentsResponse response) {
                    try {
                        XContentBuilder builder = RestXContentBuilder.restContentBuilder(request);
                        builder.startObject();

                        builder.field("cache_entries");
                        builder.startArray();
                        for (Map<String, String> cacheEntry : response.getCacheEntries()) {
                            builder.startObject();
                            for (Map.Entry<String, String> entry : cacheEntry.entrySet())
                                builder.field(entry.getKey(), entry.getValue());
                            builder.endObject();
                        }
                        builder.endArray();

                        builder.endObject();
                        channel.sendResponse(new XContentRestResponse(request, OK, builder));
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }

                @Override
                public void onFailure(Throwable e) {
                    try {
                        e.printStackTrace();
                        logger.error("CacheContents failure", e);
                        channel.sendResponse(new XContentThrowableRestResponse(request, e));
                    } catch (IOException ee) {
                        logger.error("CacheContents double failure", ee);
                    }
                }
            });
        }
    }
}

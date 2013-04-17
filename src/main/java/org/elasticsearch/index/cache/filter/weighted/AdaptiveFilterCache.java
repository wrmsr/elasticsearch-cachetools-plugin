package org.elasticsearch.index.cache.filter.weighted;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Filter;
import org.apache.lucene.util.SortedVIntList;
import org.elasticsearch.common.cache.Cache;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.lucene.docset.DocSet;
import org.elasticsearch.common.lucene.docset.DocSets;
import org.elasticsearch.common.lucene.docset.FixedBitDocSet;
import org.elasticsearch.common.lucene.search.NoCacheFilter;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.cache.filter.support.CacheKeyFilter;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.indices.cache.filter.IndicesFilterCache;

import java.io.IOException;

public class AdaptiveFilterCache extends WeightedFilterCache {

    @Inject
    public AdaptiveFilterCache(Index index, @IndexSettings Settings indexSettings,
                               IndicesFilterCache indicesFilterCache) {
        super(index, indexSettings, indicesFilterCache);
    }

    @Override
    public String type() {
        return "adaptive";
    }

    @Override
    public Filter cache(Filter filterToCache) {
        if (filterToCache instanceof NoCacheFilter) {
            return filterToCache;
        }
        if (isCached(filterToCache)) {
            return filterToCache;
        }
        return new FilterCacheFilterWrapper(filterToCache, this);
    }

    @Override
    public boolean isCached(Filter filter) {
        return filter instanceof FilterCacheFilterWrapper;
    }

    static class FilterCacheFilterWrapper extends Filter {

        private final Filter filter;

        private final WeightedFilterCache cache;

        FilterCacheFilterWrapper(Filter filter, WeightedFilterCache cache) {
            this.filter = filter;
            this.cache = cache;
        }

        @Override
        public DocIdSet getDocIdSet(IndexReader reader) throws IOException {
            Object filterKey = filter;
            if (filter instanceof CacheKeyFilter) {
                filterKey = ((CacheKeyFilter) filter).cacheKey();
            }
            FilterCacheKey cacheKey = new FilterCacheKey(this.cache, reader.getCoreCacheKey(), filterKey);
            Cache<FilterCacheKey, DocSet> innerCache = cache.indicesFilterCache.cache();

            DocSet cacheValue = innerCache.getIfPresent(cacheKey);
            if (cacheValue == null) {
                if (!cache.seenReaders.containsKey(reader.getCoreCacheKey())) {
                    Boolean previous = cache.seenReaders.putIfAbsent(reader.getCoreCacheKey(), Boolean.TRUE);
                    if (previous == null && (reader instanceof SegmentReader)) {
                        ((SegmentReader) reader).addCoreClosedListener(cache);
                        cache.seenReadersCount.inc();
                    }
                }

                cacheValue = DocSets.cacheable(reader, filter.getDocIdSet(reader));

                if (cacheValue instanceof FixedBitDocSet) {
                    FixedBitDocSet bitDocSet = (FixedBitDocSet) cacheValue;
                    if ((bitDocSet.set().cardinality() * 4) < bitDocSet.set().length()) {
                        final SortedVIntList listDocSet = new SortedVIntList(bitDocSet.iterator());
                        if (listDocSet.getByteSize() < bitDocSet.sizeInBytes()) {
                            DocSet wrappedListDocSet = new DocSet() {
                                @Override
                                public boolean get(int doc) {
                                    throw new UnsupportedOperationException();
                                }

                                @Override
                                public int length() {
                                    return listDocSet.size();
                                }

                                @Override
                                public long sizeInBytes() {
                                    return (long) listDocSet.getByteSize();
                                }

                                @Override
                                public DocIdSetIterator iterator() throws IOException {
                                    return listDocSet.iterator();
                                }
                            };

                            cacheValue = wrappedListDocSet;
                        }
                    }
                }

                // we might put the same one concurrently, that's fine, it will be replaced and the removal
                // will be called
                cache.totalMetric.inc(cacheValue.sizeInBytes());
                innerCache.put(cacheKey, cacheValue);
            }

            // return null if its EMPTY, this allows for further optimizations to ignore filters
            return cacheValue == DocSet.EMPTY_DOC_SET ? null : cacheValue;
        }

        public String toString() {
            return "cache(" + filter + ")";
        }

        public boolean equals(Object o) {
            if (!(o instanceof FilterCacheFilterWrapper)) return false;
            return this.filter.equals(((FilterCacheFilterWrapper) o).filter);
        }

        public int hashCode() {
            return filter.hashCode() ^ 0x1117BF25;
        }
    }
}

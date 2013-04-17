package com.yelp.elasticsearch.plugins.cachetools;

import org.elasticsearch.common.inject.Module;
import org.elasticsearch.plugins.AbstractPlugin;

public class CacheToolsPlugin extends AbstractPlugin {

    @Override
    public String name() {
        return "cachetools";
    }

    @Override
    public String description() {
        return "Cache Tools";
    }

    @Override
    public void processModule(Module module) {
        GetCacheContents.processModule(module);
    }
}

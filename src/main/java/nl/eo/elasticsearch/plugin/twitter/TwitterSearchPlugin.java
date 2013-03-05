package nl.eo.elasticsearch.plugin.twitter;

import org.elasticsearch.common.inject.Module;
import org.elasticsearch.plugins.AbstractPlugin;
import org.elasticsearch.rest.RestModule;

public class TwitterSearchPlugin extends AbstractPlugin {

    public String name() {
        return "twitter-search";
    }

    public String description() {
        return "Twitter Search Plugin";
    }

    @Override public void processModule(Module module) {
        if (module instanceof RestModule) {
            ((RestModule) module).addRestAction(TwitterSearchAction.class);
        }
    }
}


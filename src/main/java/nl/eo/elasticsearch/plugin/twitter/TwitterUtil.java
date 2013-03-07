package nl.eo.elasticsearch.plugin.twitter;

import org.elasticsearch.common.xcontent.support.XContentMapValues;
import java.util.Map;


import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;


public class TwitterUtil {

    private ConfigurationBuilder cb;

    public TwitterUtil(Map<String, Object> twitterSettings) {
        String oauthConsumerKey = null;
        String oauthConsumerSecret = null;
        String oauthAccessToken = null;
        String oauthAccessTokenSecret = null;

        String proxyHost = null;
        String proxyPort = null;
        String proxyUser = null;
        String proxyPassword = null;

        String user = XContentMapValues.nodeStringValue(twitterSettings.get("user"), null);
        String password = XContentMapValues.nodeStringValue(twitterSettings.get("password"), null);
        if (twitterSettings.containsKey("oauth")) {
            Map<String, Object> oauth = (Map<String, Object>) twitterSettings.get("oauth");
            if (oauth.containsKey("consumerKey")) {
                oauthConsumerKey = XContentMapValues.nodeStringValue(oauth.get("consumerKey"), null);
            }
            if (oauth.containsKey("consumer_key")) {
                oauthConsumerKey = XContentMapValues.nodeStringValue(oauth.get("consumer_key"), null);
            }
            if (oauth.containsKey("consumerSecret")) {
                oauthConsumerSecret = XContentMapValues.nodeStringValue(oauth.get("consumerSecret"), null);
            }
            if (oauth.containsKey("consumer_secret")) {
                oauthConsumerSecret = XContentMapValues.nodeStringValue(oauth.get("consumer_secret"), null);
            }
            if (oauth.containsKey("accessToken")) {
                oauthAccessToken = XContentMapValues.nodeStringValue(oauth.get("accessToken"), null);
            }
            if (oauth.containsKey("access_token")) {
                oauthAccessToken = XContentMapValues.nodeStringValue(oauth.get("access_token"), null);
            }
            if (oauth.containsKey("accessTokenSecret")) {
                oauthAccessTokenSecret = XContentMapValues.nodeStringValue(oauth.get("accessTokenSecret"), null);
            }
            if (oauth.containsKey("access_token_secret")) {
                oauthAccessTokenSecret = XContentMapValues.nodeStringValue(oauth.get("access_token_secret"), null);
            }
        }
        if (twitterSettings.containsKey("proxy")) {
            Map<String, Object> proxy = (Map<String, Object>) twitterSettings.get("proxy");
            if (proxy.containsKey("host")) {
                proxyHost = XContentMapValues.nodeStringValue(proxy.get("host"), null);
            }
            if (proxy.containsKey("port")) {
                proxyPort = XContentMapValues.nodeStringValue(proxy.get("port"), null);
            }
            if (proxy.containsKey("user")) {
                proxyUser = XContentMapValues.nodeStringValue(proxy.get("user"), null);
            }
            if (proxy.containsKey("password")) {
                proxyPassword = XContentMapValues.nodeStringValue(proxy.get("password"), null);
            }
        }

        cb = new ConfigurationBuilder();

        if (oauthAccessToken != null && oauthConsumerKey != null && oauthConsumerSecret != null && oauthAccessTokenSecret != null) {
            cb.setOAuthConsumerKey(oauthConsumerKey)
              .setOAuthConsumerSecret(oauthConsumerSecret)
              .setOAuthAccessToken(oauthAccessToken)
              .setOAuthAccessTokenSecret(oauthAccessTokenSecret);
        } else {
            cb.setUser(user).setPassword(password);
        }
        if (proxyHost != null) cb.setHttpProxyHost(proxyHost);
        if (proxyPort != null) cb.setHttpProxyPort(Integer.parseInt(proxyPort));
        if (proxyUser != null) cb.setHttpProxyUser(proxyUser);
        if (proxyPassword != null) cb.setHttpProxyHost(proxyPassword);
    }

    public String[] getUserIds(String[] screennames) {
        if (screennames.length > 0) {
            try {
                String[] retval = new String[screennames.length];
                Twitter twitter = new TwitterFactory(cb.build()).getInstance();
                ResponseList<User> users = twitter.lookupUsers(screennames);
                for (int i=0; i<users.size(); i++) {
                    User user = users.get(i);
                    retval[i] = "" + user.getId();
                }
                return retval;
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return new String[0];
    }
}

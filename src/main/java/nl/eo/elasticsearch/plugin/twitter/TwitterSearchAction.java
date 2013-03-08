package nl.eo.elasticsearch.plugin.twitter;

import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;

import org.elasticsearch.index.get.GetField;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.FilterBuilder;

import org.elasticsearch.client.Client;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;
import org.elasticsearch.common.xcontent.XContentFactory;

import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.SortOrder;

import org.elasticsearch.rest.*;

import java.io.IOException;
import java.util.*;


import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestRequest.Method.PUT;
import static org.elasticsearch.rest.RestRequest.Method.DELETE;
import static org.elasticsearch.rest.RestStatus.NOT_FOUND;
import static org.elasticsearch.rest.RestStatus.OK;
import static org.elasticsearch.rest.action.support.RestXContentBuilder.restContentBuilder;

public class TwitterSearchAction extends BaseRestHandler {

    public static String TYPE = "task";

    @Inject public TwitterSearchAction(Settings settings, Client client, RestController controller) {
        super(settings, client);
        // Define REST endpoints

        // Get a list of all tasks, mapped to 'listTasks'
        controller.registerHandler(GET, "/_twittersearch/{index}", this);

        // Get a specific task, mapped to 'getTask'
        controller.registerHandler(GET, "/_twittersearch/{index}/{id}", this);

        // Get tweets for a specific task, mapped to 'getTweetTask'
        controller.registerHandler(GET, "/_twittersearch/{index}/{id}/{tweets}", this);

        // Delete a specific task, mapped to 'deleteTask'
        controller.registerHandler(DELETE, "/_twittersearch/{index}/{id}", this);

        // Add a task, mapped to 'addTask'
        controller.registerHandler(POST, "/_twittersearch/{index}/", this);

        // Update a specific task
        controller.registerHandler(POST, "/_twittersearch/{index}/{id}", this);
    }

    private void updateRiver(String index) {
        logger.info("Updating river");
        GetResponse taskResponse = client.prepareGet(index, "twitterconfig", "_meta").execute().actionGet();
        Map<String, Object> twitterConfig = taskResponse.getSource();

        final SearchRequest searchRequest = new SearchRequest(index);
        searchRequest.types(TYPE);
        searchRequest.listenerThreaded(false);
        SearchResponse searchResponse = client.prepareSearch(index).setTypes(TYPE).execute().actionGet();

        Set<String> toFollow = new TreeSet<String>();
        Set<String> toTrack = new TreeSet<String>();

        for (SearchHit hit : searchResponse.hits()) {
            Map<String, Object> source = hit.getSource();
            String follow[] = Strings.commaDelimitedListToStringArray((String)source.get("follow"));
            String track[] = Strings.commaDelimitedListToStringArray((String)source.get("track"));
            toFollow.addAll(Arrays.asList(follow));
            toTrack.addAll(Arrays.asList(track));
        }
        logger.info("Tracking userids {} and keywords {}", toFollow, toTrack);

        TwitterUtil tu = new TwitterUtil(twitterConfig);
        List<String> toFollowIDs = Arrays.asList(tu.getUserIds(toFollow.toArray(new String[0])));

/*
        DeleteResponse response = client.prepareDelete("_river", "twitter", "_meta")
           .setRefresh(true)
           .execute().actionGet();
           */

        ClusterStateResponse resp = client.admin().cluster().prepareState().execute().actionGet();
        Map mappings = resp.state().metaData().index("_river").mappings();
        if (mappings.containsKey("twitter")) {
            logger.info("Old twitter river exists, we will delete it");
            client.admin().indices().prepareDeleteMapping("_river").setType("twitter").execute().actionGet();
            logger.info("Removed old river");
        } else {
            logger.info("No old twitter river, so no need to delete the mapping");
        }

        // Put the entire twitter configuration (partially recovered from the _meta key, partially created by our logic)
        // into a hashmap
        Map t = new HashMap();
        t.put("tracks", Strings.collectionToCommaDelimitedString(toTrack));
        t.put("follow", Strings.collectionToCommaDelimitedString(toFollowIDs));
        twitterConfig.put("filter", t);

        try {
            XContentBuilder builder = XContentFactory.jsonBuilder()
              .startObject()
                .field("type", "twitter")
                .field("twitter", twitterConfig)
                .startObject("index")
                  .field("index", index)
                  .field("type", "status")
                  .field("bulk_size", 1)
                .endObject()
              .endObject();

            IndexResponse response2 = client.prepareIndex("_river", "twitter", "_meta")
                .setSource(builder)
                .setRefresh(true)
                .execute().actionGet();

            logger.info("Created new river");
        } catch (IOException e) {
            logger.error("Exception while building new river: {}", e);
        }
    }

    private void historicSearch(String id) {
      // do a historic search for the given tas, so we can populate the index with some content
    }

    private String[] findUserIds(String screennames, Map<String, Object> config) {
        TwitterUtil tu = new TwitterUtil(config);
        return tu.getUserIds(Strings.commaDelimitedListToStringArray(screennames));
    }

    private void deleteTask(final RestRequest request, final RestChannel channel, final String index, final String id) {
        String type = "_meta".equals(id) ? "twitterconfig" : TYPE;
        DeleteResponse response = client.prepareDelete(index, type, id)
          .setRefresh(true)
          .execute().actionGet();
        try {
            XContentBuilder builder = restContentBuilder(request)
              .startObject()
                .field("id", response.getId())
                .field("version", response.getVersion())
              .endObject();
            channel.sendResponse(new XContentRestResponse(request, OK, builder));
        } catch (Exception e) {
            try {
                channel.sendResponse(new XContentThrowableRestResponse(request, e));
            } catch (IOException e1) {
                logger.error("Failed to send failure response", e1);
            }
        }
        updateRiver(index);
    }

    private void listTasks(final RestRequest request, final RestChannel channel, final String index) {
        final SearchRequest searchRequest = new SearchRequest(index);
        searchRequest.types(TYPE);
        searchRequest.listenerThreaded(false);

        SearchResponse searchResponse = client.prepareSearch(index).setTypes(TYPE).execute().actionGet();
        try {
            XContentBuilder builder = restContentBuilder(request);
            builder.startObject();
            searchResponse.toXContent(builder, request);
            builder.endObject();
            channel.sendResponse(new XContentRestResponse(request, OK, builder));
        } catch (Throwable e) {
            try {
                e.printStackTrace();
                channel.sendResponse(new XContentThrowableRestResponse(request, e));
            } catch (IOException e1) {
                logger.error("Failed to send failure response", e1);
            }
        }

    }

    private void addTask(final RestRequest request, final RestChannel channel, final String index) {
        IndexResponse response = client.prepareIndex(index, TYPE)
          .setSource(request.content().toBytes())
          .setRefresh(true)
          .execute().actionGet();
        try {
            XContentBuilder builder = restContentBuilder(request)
              .startObject()
                .field("id", response.getId())
              .endObject();
            channel.sendResponse(new XContentRestResponse(request, OK, builder));
        } catch (Exception e) {
            try {
                channel.sendResponse(new XContentThrowableRestResponse(request, e));
            } catch (IOException e1) {
                logger.error("Failed to send failure response", e1);
            }
        }

        historicSearch(response.getId());
        updateRiver(index);
    }

    private void updateTask(final RestRequest request, final RestChannel channel, final String index, final String id) {
        String type = "_meta".equals(id) ? "twitterconfig" : TYPE;
        IndexResponse response = client.prepareIndex(index, type, id)
          .setSource(request.content().toBytes())
          .setRefresh(true)
          .execute().actionGet();
        try {
            XContentBuilder builder = restContentBuilder(request)
              .startObject()
                .field("id", response.getId())
                .field("version", response.getVersion())
              .endObject();
            channel.sendResponse(new XContentRestResponse(request, OK, builder));
        } catch (Exception e) {
            try {
                channel.sendResponse(new XContentThrowableRestResponse(request, e));
            } catch (IOException e1) {
                logger.error("Failed to send failure response", e1);
            }
        }

        historicSearch(response.getId());
        updateRiver(index);
    }
    

    private void getTweets(final RestRequest request, final RestChannel channel, final String index, final String id, final String tweets) {
        GetResponse taskResponse = client.prepareGet(index, TYPE, id).execute().actionGet();
        Map<String, Object> task = taskResponse.getSource();

        String follow = (String)task.get("follow");
        String track = (String)task.get("track");
        String block = "";

        try {
            // Get the response, and corresponding fields
            // Do a search in Elastic
            // Return the results
            ArrayList<FilterBuilder> mayMatch = new ArrayList<FilterBuilder>();
            if (!"".equals(follow)) {
              mayMatch.add(FilterBuilders.termFilter("user.screen_name", Strings.commaDelimitedListToStringArray(follow)));
            }
            if (!"".equals(track)) {
              mayMatch.add(FilterBuilders.termFilter("hashtag.text", Strings.commaDelimitedListToStringArray(track)));
            }
            ArrayList<FilterBuilder> mustMatch = new ArrayList<FilterBuilder>();

            mustMatch.add(FilterBuilders.orFilter(mayMatch.toArray(new FilterBuilder[0])));
            if (!"".equals(block)) {
              mustMatch.add(
                FilterBuilders.notFilter(
                  FilterBuilders.termFilter("user.screen_name", Strings.commaDelimitedListToStringArray(block))
                )
              );
            }

            FilterBuilder filter = FilterBuilders.andFilter(mustMatch.toArray(new FilterBuilder[0]));

            SearchResponse response = client.prepareSearch(index)
                    .setTypes("status")
                    .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                    .setFilter(filter)
                    .setFrom(0).setSize(10).setExplain(true)
                    .addSort("created_at", SortOrder.DESC)
                    .execute()
                    .actionGet();

            XContentBuilder builder = XContentFactory.jsonBuilder();
            builder.startObject();
            builder.field("completed_in", ((float)response.tookInMillis() / 1000));
            if (response.hits().totalHits() > 0) {
                builder.field("max_id", Long.decode(response.hits().getAt(0).id()).longValue());
                builder.field("max_id_str", response.hits().getAt(0).id());
            }
            builder.startArray("results");
            for (SearchHit hit : response.hits()) {
                builder.startObject();
                Map<String, Object> source = hit.getSource();
                builder.field("created_at", source.get("created_at"));
                builder.field("from_user", ((Map)source.get("user")).get("screen_name"));
                builder.field("from_user_id", ((Map)source.get("user")).get("id"));
                builder.field("from_user_id_str", ((Map)source.get("user")).get("id"));
                builder.field("from_user_name", ((Map)source.get("user")).get("name"));
                builder.field("profile_image_url", ((Map)source.get("user")).get("profile_image_url"));
                builder.field("profile_image_url_https", ((Map)source.get("user")).get("profile_image_url_https"));
                builder.field("id", Long.decode(hit.id()).longValue());
                builder.field("id_str", hit.id());
                builder.field("text", source.get("text"));
                builder.field("source", source.get("source"));
                builder.endObject();
            }
            builder.endArray();
            builder.endObject();
            channel.sendResponse(new XContentRestResponse(request, OK, builder));

        } catch (Exception e) {
            e.printStackTrace();
            try {
                channel.sendResponse(new XContentThrowableRestResponse(request, e));
            } catch (IOException e1) {
                logger.error("Failed to send failure response", e1);
            }
        }
    }

    private void getTask(final RestRequest request, final RestChannel channel, final String index, final String id) {
        String type = "_meta".equals(id) ? "twitterconfig" : TYPE;
        GetResponse response = client.prepareGet(index, type, id).execute().actionGet();
        try {
            XContentBuilder builder = restContentBuilder(request);
            response.toXContent(builder, request);

            if (!response.exists()) {
                channel.sendResponse(new XContentRestResponse(request, NOT_FOUND, builder));
            } else {
                channel.sendResponse(new XContentRestResponse(request, OK, builder));
            }
        } catch (Exception e) {
            try {
                channel.sendResponse(new XContentThrowableRestResponse(request, e));
            } catch (IOException e1) {
                logger.error("Failed to send failure response", e1);
            }
        }
    }

    public void handleRequest(final RestRequest request, final RestChannel channel) {
        final String index = request.hasParam("index") ? request.param("index") : "";
        final String id = request.hasParam("id") ? request.param("id") : "";
        final String tweets = request.hasParam("tweets") ? request.param("tweets") : "";
        logger.info("Request: {} {} {} {}", request.method(), index, id, tweets);

        switch(request.method()) {
            case DELETE:
                deleteTask(request, channel, index, id);
                break;
            case GET:
                if ("".equals(id)) {
                    listTasks(request, channel, index);
                } else {
                    if ("".equals(tweets)) {
                        getTask(request, channel, index, id);
                    } else {
                        getTweets(request, channel, index, id, tweets);
                    }
                }
                break;
            case POST:
            case PUT:
                if ("".equals(id)) {
                    addTask(request, channel, index);
                } else if ("_search".equals(id)) {
                    listTasks(request, channel, index);
                } else {
                    updateTask(request, channel, index, id);
                }
                break;
        }

    }
}

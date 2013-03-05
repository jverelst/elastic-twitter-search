package nl.eo.elasticsearch.plugin.twitter;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;

import org.elasticsearch.client.Client;
import org.elasticsearch.index.get.GetField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;
import org.elasticsearch.rest.*;

import java.io.IOException;

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

        // Delete a specific task, mapped to 'deleteTask'
        controller.registerHandler(DELETE, "/_twittersearch/{index}/{id}", this);

        // Add a task, mapped to 'addTask'
        controller.registerHandler(PUT, "/_twittersearch/{index}/", this);

        // Update a specific task
        controller.registerHandler(PUT, "/_twittersearch/{index}/{id}", this);
    }

    private void deleteTask(final RestRequest request, final RestChannel channel, final String index, final String id) {
        // TODO:
        // - Delete the task
        // - Fetch all tasks, create river configuration
        // - Re-initialize the river
    }

    private void listTasks(final RestRequest request, final RestChannel channel, final String index) {
        // TODO:
        // - Fetch all tasks, return data
        logger.info("listTasks()");
        final SearchRequest searchRequest = new SearchRequest(index);
        searchRequest.types(TYPE);
        searchRequest.listenerThreaded(false);

        SearchResponse searchResponse = client.prepareSearch(index).setTypes(TYPE).execute().actionGet();
        try {
            logger.info("Hits: " + searchResponse.hits().totalHits());
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

        logger.info("listTasks() finished");
    }

    private void addTask(final RestRequest request, final RestChannel channel, final String index) {
        // TODO:
        // - Add the task
        // - Fetch all tasks, create river configuration
        // - Re-initialize the river
        // - Do a historic search
        // - 
    }

    private void updateTask(final RestRequest request, final RestChannel channel, final String index, final String id) {
        // TODO:
        // - Update the task
        // - Fetch all tasks, create river configuration
        // - Re-initialize the river
    }
    

    private void getTask(final RestRequest request, final RestChannel channel, final String index, final String id) {
        GetResponse response = client.prepareGet(index, TYPE, id).setFields("keywords", "users", "follow", "ignore").execute().actionGet();
        try {
            logger.info("exists? " + response.exists());
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
        logger.info("TwitterSearchAction.handleRequest called");
        final String index = request.hasParam("index") ? request.param("index") : "";
        final String id = request.hasParam("id") ? request.param("id") : "";
        logger.info("Index: " + index + ", ID: " + id + ", method: " + request.method());

        switch(request.method()) {
            case DELETE:
                deleteTask(request, channel, index, id);
                break;
            case GET:
                if ("".equals(id)) {
                    listTasks(request, channel, index);
                } else {
                    getTask(request, channel, index, id);
                }
                break;
            case PUT:
                if ("".equals(id)) {
                    addTask(request, channel, index);
                } else {
                    updateTask(request, channel, index, id);
                }
                break;
        }

    }
}

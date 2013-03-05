package nl.eo.elasticsearch.plugin.twitter;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;

import org.elasticsearch.client.Client;
import org.elasticsearch.index.get.GetField;
import org.elasticsearch.common.Strings;
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

    public static String INDEX = "_twittersearch";
    public static String TYPE = "task";

    @Inject public TwitterSearchAction(Settings settings, Client client, RestController controller) {
        super(settings, client);

        // Define REST endpoints

        // Get a list of all tasks, mapped to 'listTasks'
        controller.registerHandler(GET, "/_twittersearch/", this);

        // Get a specific task, mapped to 'getTask'
        controller.registerHandler(GET, "/_twittersearch/{id}", this);

        // Delete a specific task, mapped to 'deleteTask'
        controller.registerHandler(DELETE, "/_twittersearch/{id}", this);

        // Add a task, mapped to 'addTask'
        controller.registerHandler(PUT, "/_twittersearch/", this);

        // Update a specific task
        controller.registerHandler(PUT, "/_twittersearch/{id}", this);
    }

    private void deleteTask(final RestRequest request, final RestChannel channel, final String id) {

    }


    private void listTasks(final RestRequest request, final RestChannel channel) {

    }

    private void addTask(final RestRequest request, final RestChannel channel) {

    }

    private void updateTask(final RestRequest request, final RestChannel channel, final String id) {

    }
    

    private void getTask(final RestRequest request, final RestChannel channel, final String id) {
        final GetRequest getRequest = new GetRequest(INDEX, TYPE, id);
        getRequest.listenerThreaded(false);
        getRequest.operationThreaded(true);

        String[] fields = {"keywords", "users"};
        getRequest.fields(fields);

        client.get(getRequest, new ActionListener<GetResponse>() {
            @Override public void onResponse(GetResponse response) {

                try {
                    XContentBuilder builder = restContentBuilder(request);
                    GetField field = response.field("msg");
                    String greeting = (field!=null) ? (String)field.values().get(0) : "Sorry, do I know you?";
                    builder
                        .startObject()
                        .field(new XContentBuilderString("hello"), id)
                        .field(new XContentBuilderString("greeting"), greeting)
                        .endObject();

                    if (!response.exists()) {
                        channel.sendResponse(new XContentRestResponse(request, NOT_FOUND, builder));
                    } else {
                        channel.sendResponse(new XContentRestResponse(request, OK, builder));
                    }
                } catch (Exception e) {
                    onFailure(e);
                }
            }

            @Override public void onFailure(Throwable e) {
                try {
                    channel.sendResponse(new XContentThrowableRestResponse(request, e));
                } catch (IOException e1) {
                    logger.error("Failed to send failure response", e1);
                }
            }
        });

    }

    public void handleRequest(final RestRequest request, final RestChannel channel) {
        logger.debug("TwitterSearchAction.handleRequest called");
        final String id = request.hasParam("id") ? request.param("id") : "";

        switch(request.method()) {
            case DELETE:
                deleteTask(request, channel, id);
                break;
            case GET:
                if ("".equals(id)) {
                    listTasks(request, channel);
                } else {
                    getTask(request, channel, id);
                }
                break;
            case PUT:
                if ("".equals(id)) {
                    addTask(request, channel);
                } else {
                    updateTask(request, channel, id);
                }
                break;
        }

    }
}

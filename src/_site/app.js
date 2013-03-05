var App = Em.Application.create({
  name: "Tasks",

  Models:      Ember.Object.extend(),
  Views:       Ember.Object.extend(),
  Controllers: Ember.Object.extend(),

  // Sniff the URL if we're running as an elasticsearch `_site` plugin
  //
  elasticsearch_url: function() {
    var location = window.location
    return (/_plugin/.test(location.href.toString())) ? location.protocol + "//" + location.host : "http://localhost:9200"
  }(),

  ready: function() {
    var index_url = [App.store.adapter.url, "tasks"].join('/');

    // Let's check if the `tasks` index exists...
    //
    jQuery.ajax({
      url:   index_url,
      type:  'HEAD',
      error: function(xhr, textStatus, error) {
        // ... elasticsearch appears to be down (no response)
        //
        if ( ""          == error ) App.set("elasticsearch_unavailable", true);
        //
        // ... elasticsearch is up but the index is missing, let's create it
        //
        if ( "Not Found" == error ) jQuery.post(index_url, {}, function(data) {});
      }
    });
  }
});

// Define a store for the application
//
App.store = DS.Store.create({
  revision: 4,
  adapter: DS.ElasticSearchAdapter.create({url: App.elasticsearch_url})
});

// Define the model using _Ember Data_ API
//
App.Models.Task = DS.Model.extend({
  // Properties:
  //
  keywords:   DS.attr('string'),
  follow:     DS.attr('string'),
  ignore:     DS.attr('string'),

  // Observe changes and persist them in elasticsearch
  //
  changed: function() {
    App.store.commit();
  }
});

App.Models.Task.reopenClass({
  // Define the index and type for elasticsearch
  //
  url: 'tasks/task'
});


/*
curl -XDELETE localhost:9200/_river/twitter
curl -XPUT localhost:9200/_river/twitter/_meta -d '
 {
     "type" : "twitter",
     "twitter" : {
         "oauth" : {
             "consumerKey" : "OnuxrQuz6ezYjzEWPMsA",
             "consumerSecret" : "DzzIl3w3QVvAEnZaDaEufxAR683g540qnBhfhF9CD0",
             "accessToken" : "3702441-rAWK1E0dudnZmsHFe6odubyuOWDh2KeomBCEHbhNNE",
             "accessTokenSecret" : "ZbvOMntDcnFbcIHIr895q7hdrjVb1lpx2RQgUcU"
         },
         "filter" : {
             "tracks" : "eojd,vakantie,ooit"
         }
     },
     "index" : {
         "index" : "twitter",
         "type" : "status",
         "bulk_size" : 1
     }
 }
'

*/

App.Controllers.tasks = Ember.ArrayController.create({
  // TODO: Display sorted with `sortProperties`,
  //       currently fails with `Cannot read property 'length' of undefined` @ ember-1.0.pre.js:18675
  //
  content: App.Models.Task.find(),

  updateRiver: function() {
    var tasks = App.Models.Task.find();
    var cnt = 0;
    var allFollow = new Ember.Set();
    var allKeywords = new Ember.Set();

    tasks.forEach(function(item, index, enumerable) {
      var follow = item.get("follow").split(",");
      var keywords = item.get("keywords").split(",");
      allFollow.addObjects(follow);
      allKeywords.addObjects(keywords);
    });

    var follow = allFollow.toArray();
    var keywords = allKeywords.toArray();

    $.ajax({
      type: 'GET',
      url: App.elasticsearch_url + "/_river/twitter/_meta",
      dataType: 'json',
      success: function (r) {
        var source = r._source;
        var oldTracks = source.twitter.filter.tracks;
        var oldFollow = source.twitter.filter.follow;

        source.twitter.filter.tracks = keywords.join(",");
        source.twitter.filter.follow = follow.join(",");

        $.ajax({
          type: 'DELETE',
          url: '/_river/twitter',
          success: function(r2) {
            $.ajax({
              type: 'PUT',
              url: '/_river/twitter/_meta',
              data: JSON.stringify(source),
              dataType: 'json',
              succes: function(r3) {
              },
              error: function (r4) {
                source.twitter.filter.tracks = oldFollow;
                source.twitter.filter.follow = oldTracks;
                $.ajax({
                  type: 'PUT',
                  url: '/_river/twitter/_meta',
                  data: JSON.stringify(source),
                  dataType: 'json',
                  succes: function(r3) {
                  }
                });
              }
            });
          }
        });
      }
    });
  },

  createTask: function(value, value2, value3) {
    var task = App.Models.Task.createRecord({ keywords: value, follow: value2, ignore: value3 });
    App.store.commit();
    App.Controllers.tasks.updateRiver();
  },

  removeTask: function(event) {
    if ( confirm("Delete this searchquery?") ) {
      var task = event.context;
      task.deleteRecord();
      App.store.commit();
      App.Controllers.tasks.updateRiver();
    }
  }
});

App.CreateView = Ember.View.extend({
  create: function() {
    App.Controllers.tasks.createTask(this.get('keywords').get('value'), this.get('follow').get('value'), this.get('ignore').get('value'));
  }
});

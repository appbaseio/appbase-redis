var async = require('async')
var redis = require('redis')

var db
var sub_client

var event_handler = {}
var subCounts = {};

var wait_for_change = function wait_for_change(collection_name, id, listen, callback) {
    var channel = collection_name + '/' + id;

    var handler = function(incoming_channel, message) {
        if(incoming_channel === channel) {
            callback(null, JSON.parse(message));
        }
    };

    var unsubscribe = function unsubscribe() {
        sub_client.removeListener('message', handler);
        subCounts[channel] = subCounts[channel] - 1;
        if(subCounts[channel] === 0) {
            sub_client.unsubscribe(channel);
        }
    };

    sub_client.on('message', handler);

    listen.once('off', unsubscribe);

    if(!subCounts[channel]) {
        subCounts[channel] = 1;
        sub_client.subscribe(channel);
    } else {
        subCounts[channel] = subCounts[channel] + 1;
    }
};

event_handler.connect = function connect(port, hostname, db_temp) {
  db = db_temp

  sub_client = redis.createClient(port, hostname, {})
  sub_client.on("error", function (err) {
    console.log("Error " + err)
  })
}

var publish_event = function publish_event(backReference, incoming, callback) {
    var splitReference = backReference.split("/")
    if (splitReference.length > 2) {
        var result = {}
        result["/"+splitReference[2]] = incoming
        db.client.publish(splitReference[0]+'/'+splitReference[1], JSON.stringify(result))
    } else {
        console.log("Back Reference Error: ", backReference)
    }
}

event_handler.send_event = function send_event(collection_name, name, result) {
    db.client.publish(collection_name+'/'+name, JSON.stringify(result))

    async.waterfall([
        function(callback) {
            db._getBackReferences(collection_name, name, callback)
        }, 
        function(backReferences, callback) {
            async.eachLimit(backReferences, 15, function(backReference, callback) {
                publish_event(backReference, result, callback)
                callback(null)
            }, callback)
        }
    ], function(err) {
        if(err) {
            console.log("Event Error: " + err.message)
        }
    })
}

event_handler.receive_events = function receive_events(collection_name, name, path, listen, callback) {
    async.waterfall([
        function(callback) {
            db.traverse(collection_name, name, path, callback)
        }
    ], function(err, collection_name, name) {
        wait_for_change(collection_name, name, listen, callback)
    })
}

module.exports = event_handler
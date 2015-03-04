var async = require('async')
var grip = require('grip')

var db = require(__dirname + '/appbase_redis')
db.connect(6379, '127.0.0.1')
var event_handler = require(__dirname + '/event_handler')
event_handler.connect(6379, '127.0.0.1', db)

request_handlers = {}

request_handlers.notFound = function notFound(request, reply) {
    return reply({
        "message": "This is not a valid API call"
    }).code(404)
}

var resultHandler = function resultHandler(err, result, reply) {
    if (err) {
        var code = 500
        if (err.status_code) {
            code = err.status_code
            delete err.status_code
        }
        return reply(err).code(code)
    } else {
        reply(result)
    }
}

request_handlers.listCollections = function listCollections(request, reply) {
    async.waterfall([
        db.getCollections,
    ], function(err, result) {
        resultHandler(err, result, reply)
    })
}

request_handlers.createCollection = function createCollection(request, reply) {
    async.waterfall([
        function(callback) {
            db.createCollection(request.params.collection, callback)
        },
    ], function(err, result) {
        resultHandler(err, result, reply)
    })
}

request_handlers.deleteCollection = function deleteCollection(request, reply) {
    async.waterfall([
        function(callback) {
            db.deleteCollection(request.params.collection, callback)
        },
    ], function(err, result) {
        resultHandler(err, result, reply)
    })
}

request_handlers.createDocument = function createDocument(request, reply) {
    if ( !(request.payload && typeof(request.payload) == 'object') ) {
        var err = new Error("Request body is not JSON")
        err.status_code = 400
        return resultHandler(err, null, reply)
    }
    var body = request.payload
    var id = body._id
    delete body._id
    delete body._collection
    delete body._timestamp
    async.waterfall([
        function(callback) {
            db.createDocument(request.params.collection, id, body, callback)
        },
    ], function(err, result) {
        resultHandler(err, result, reply)
    })
}

/*request_handlers.getCollection = function getCollection(request, reply) {
    async.waterfall([
        function(callback) {
            db.getCollection(request.params.collection, callback)
        },
    ], function(err, result) {
        resultHandler(err, result, reply)
    })
}*/

request_handlers.getDocument = function getDocument(request, reply) {
    async.waterfall([
        function(callback) {
            var edgepath = db._pathResolution(request.params.edgepath)
            db.traverse(request.params.collection, request.params.rootdoc, edgepath, callback)
        },
        function(collection_name, name, callback) {
            var timestamp = -1
            if (!isNaN(request.params.timestamp)) {
                timestamp = parseInt(request.params.timestamp)
            }
            var references = true
            if (request.params.references == "false") {
                references = false
            }
            db.getDocument(collection_name, name, references, timestamp, callback)
        }
    ], function(err, result) {
        resultHandler(err, result, reply)
    })
}

request_handlers.updateDocument = function updateDocument(request, reply) {
    var collection_name, name
    async.waterfall([
        function(callback) {
            var edgepath = db._pathResolution(request.params.edgepath)
            db.traverse(request.params.collection, request.params.rootdoc, edgepath, callback)
        },
        function(collection_name_temp, name_temp, callback) {
            collection_name = collection_name_temp
            name = name_temp
            var body = request.payload
            delete body._id
            delete body._collection
            delete body._timestamp
            db.updateDocument(collection_name, name, body, callback)
        }
    ], function(err, result) {
        event_handler.send_event(collection_name, name, result)
        resultHandler(err, result, reply)
    })
}

request_handlers.deleteDocument = function deleteDocument(request, reply) {
    async.waterfall([
        function(callback) {
            var edgepath = db._pathResolution(request.params.edgepath)
            db.traverse(request.params.collection, request.params.rootdoc, edgepath, callback)
        },
        function(collection_name, name, callback) {
            db.deleteDocument(collection_name, name, callback)
        }
    ], function(err, result) {
        event_handler.send_event(collection_name, name, result)
        resultHandler(err, result, reply)
    })
}

module.exports = request_handlers
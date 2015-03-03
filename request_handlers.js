var async = require('async')
var grip = require('grip')

var db = require(__dirname + '/appbase_redis')

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
        return reply(result).code(code)
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
            db.traverse(request.params.collection, request.params.rootdoc, request.params.edgepath, callback)
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
    async.waterfall([
        function(callback) {
            db.traverse(request.params.collection, request.params.rootdoc, request.params.edgepath, callback)
        },
        function(collection_name, name, callback) {
            var body = request.payload
            delete body._id
            delete body._collection
            delete body._timestamp
            db.updateDocument(collection_name, name, body, callback)
        }
    ], function(err, result) {
        resultHandler(err, result, reply)
    })
}

request_handlers.deleteDocument = function deleteDocument(request, reply) {
    async.waterfall([
        function(callback) {
            db.traverse(request.params.collection, request.params.rootdoc, request.params.edgepath, callback)
        },
        function(collection_name, name, callback) {
            db.deleteDocument(collection_name, name, callback)
        }
    ], function(err, result) {
        resultHandler(err, result, reply)
    })
}

module.exports = request_handlers
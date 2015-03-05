var Hapi = require('hapi')
var Good = require('good')

var rh = require(__dirname + '/request_handlers')

var server = new Hapi.Server()
server.connection({
    port: parseInt(process.env.PORT, 10) || 3000,
    routes: {
        cors: {
            methods: ['GET', 'HEAD', 'POST', 'PATCH', 'DELETE', 'OPTIONS']
        }
    }
})

var collectionPath = '/{collection}'
var documentPath = '/{collection}/{rootdoc}/{edgepath*}'

server.route({
    method: '*',
    path: '/{p*}',
    handler: rh.notFound
})

server.route({
    method: 'GET',
    path: '/',
    handler: rh.listCollections
})

server.route({
    method: 'PATCH',
    path: collectionPath,
    handler: rh.createCollection
})
server.route({
    method: 'DELETE',
    path: collectionPath,
    handler: rh.deleteCollection
})
server.route({
    method: 'POST',
    path: collectionPath,
    handler: rh.createDocument
})
server.route({
    method: 'GET',
    path: collectionPath,
    handler: rh.getCollection
})

server.route({
    method: 'GET',
    path: documentPath,
    handler: rh.getDocument
})
server.route({
    method: 'PATCH',
    path: documentPath,
    handler: rh.updateDocument
})
server.route({
    method: 'DELETE',
    path: documentPath,
    handler: rh.deleteDocument
})

server.register({
    register: Good,
    options: {
        reporters: [{
            reporter: require('good-console'),
            args:[{ log: '*', response: '*' }]
        }]
    }
}, function (err) {
    if (err) {
        throw err // something bad happened loading the plugin
    }

    server.start(function () {
        server.log('info', 'Server running at: ' + server.info.uri)
    })
})

var expect = require("chai").expect,
    ar = require("./appbase_redis.js")

describe("Internal tests", function() {
  describe("server timestamp", function() {
    it("should return the time datatypes correctly", function() {
      var serverTime = ar._serverTime()
      expect(serverTime).to.have.property('_timestamp').to.be.a('number')
      expect(serverTime).to.have.property('_timezoneOffset').to.be.a('number')
      expect(serverTime).to.have.property('_ISOString').to.be.a('string')
    })
  })

  describe("Document parser", function() {
    it("shoud parse properties and references correctly", function(done) {
      // nothing here so far.
      var exampleRequestBody = {
        'hello' : 'js',
        'everything': true,
        '/foo' : {
            '_id': 'anythingworks',
            '_collection': 'testcollection4238',
            'this': 'is awesome',
            'life': 42
        }
      }
      ar.updateDocument('foobar12', 'dabang', exampleRequestBody, function(err, res) {console.log(err, res)})
      ar._parseDocumentBody(exampleRequestBody, function(err, res) {
        if (err) throw err;
        expect(res).to.have.property('properties').to.have.property('hello').to.equal('js')
        expect(res).to.have.property('properties').to.have.property('everything').to.be.ok
        expect(res).to.have.property('references').to.have.property('foo').to.have.property('_id').to.equal('anythingworks')
        expect(res).to.have.property('references').to.have.property('foo').to.have.property('_collection').to.equal('testcollection4238')
        expect(res).to.have.property('references').to.have.property('foo').to.have.property('this').to.equal('is awesome')
        expect(res).to.have.property('references').to.have.property('foo').to.have.property('life').to.equal(42)
        done()
      })
    })

    it("should throw an Error when giving incorrect arguments", function() {
      expect(ar._parseDocumentBody).to.throw(Error)
    })

    it("should throw an Error when not passing a JavaScript Object type", function(done) {
      ar._parseDocumentBody(null, function(err, res) {
        if (err) console.log(err);
        done();
      })
    })
  })

  describe("Path traverser", function() {
    it("should traverse the paths correctly and return collection/document", function(done) {
      ar.traverse("foobar12", "dabang", ["foo"], function(err, collection, name) {
        if (err) throw err;
        expect(collection).to.equal('testcollection4238')
        expect(name).to.equal('anythingworks')
        done()
      })
    })
    it("should not traverse the paths correctly and return collection/document", function(done) {
      ar.traverse("foobar12", "dabang", ["foo", "bar"], function(err, collection, name) {
        if (err) console.log(err);
        done();
      })
    })
  })

  describe("Get Document Back References", function() {
    it("should return the back references of a document as an array of col/doc/ref format", function(done) {
      ar._getBackReferences("testcollection4238", "anythingworks", function(err, res) {
        if (err) throw err;
        expect(res).to.be.an("Array")
        console.log(res)
        done();
      })
    })
  })
})

describe("Collection tests", function() {
  var name = "testcollection1234"

  describe("create collection", function() {
    it("should create a collection with the current server timestamp", function(done) {
      ar.createCollection(name, function(err, res) {
        if (err) throw err;
        expect(res).to.have.property('_collection').to.equal(name)
        expect(res).to.have.property('_createdAt').to.be.a('number')
        done();
      })
    })
  })

  describe("get all collections", function() {
    it("should return all the collections", function(done) {
      ar.getCollections(function(err, res) {
        if (err) throw err;
        expect(res).to.have.property('_collections').to.be.a('Array')
        expect(res).to.have.property('_collections').to.have.length.at.least(1)
        done();
      })
    })
  })

  describe("delete collection", function() {
    it("should delete the collection with the given key", function(done) {
      ar.deleteCollection(name, function(err, res) {
        if (err) throw err;
        expect(res).to.have.property('_collection').to.equal(name)
        expect(res).to.have.property('_deleted').to.be.ok
        done()
      })
    })
  })
})

describe("Document tests", function() {

  var exampleRequestBody = {
    'hello' : 'js',
    'everything': true,
    '/foo' : {
        '_id': 'anythingworks',
        '_collection': 'testcollection4238',
        'this': 'is awesome',
        'life': 42
    }
  }

  describe("Create / Update a document", function() {
    it("should create document and reference. reference depth=1, new document=true", function(done) {
      ar.updateDocument('hello', 'trythis', exampleRequestBody, function(err, res) {
        if (err) throw err;
        expect(res).to.have.property('hello').to.equal('js')
        expect(res).to.have.property('everything').to.equal(true)
        expect(res).to.have.property('/foo')
        expect(res).to.have.property('/foo').to.have.property('_id').to.equal('anythingworks')
        expect(res).to.have.property('/foo').to.have.property('_collection').to.equal('testcollection4238')
        expect(res).to.have.property('/foo').to.have.property('this').to.equal('is awesome')
        expect(res).to.have.property('/foo').to.have.property('life').to.equal(42)
        done();
      })
    })
    it("should create document and reference. reference depth=1, new document=false", function(done) {
      exampleRequestBody['/foo'] = 'foobar12/dabang'
      ar.updateDocument('foobar12', 'dabang', exampleRequestBody, function(err, res) {
        if (err) throw err;

        expect(res).to.have.property('hello').to.equal('js')
        expect(res).to.have.property('everything').to.equal(true)
        expect(res).to.have.property('/foo').to.equal('foobar12/dabang')
        done();
      })
    })
  })

  describe("Get a document", function() {
    it("should return an existing document", function(done) {
      ar.getDocument('foobar12', 'dabang', true, false, function(err, res) {
        if (err) throw err;
        expect(res).to.have.property('hello').to.equal('js')
        done();
      })
    })
  })
})

var redis = require("redis")
var client

var functions = {}

/* Collection methods - create, delete, get */

functions.connect = function connect(port, hostname) {
  client = redis.createClient(port, hostname, {})
  client.on("error", function (err) {
    console.log("Error " + err)
  })
  functions.client = client
}

functions.createCollection = function createCollection(name, done) {
  var timestamp = Date.now()
  client.zadd("collections", timestamp, name, function(err, res) {
    if (err) {
      done(err)
    } else {
      done(null, {"_collection": name, "_createdAt": timestamp})
    }
  })
}

functions.deleteCollection = function deleteCollection(name, done) {
  client.zrem("collections", name, function(err, res) {
    if (err) {
      done(err)
    } else {
      done(null, {"_collection": name, "_deleted": true})
    }
  })
}

functions.getCollections = function getCollections(done) {
  client.zrangebyscore("collections", "-inf", "+inf", function(err, res) {
    if (err) {
      done(err)
    } else {
      done(null, {"_time": functions._serverTime(), "_collections": res})
    }
  })
}

/* Document methods - create, update, delete, get */

functions.updateDocument = function updateDocument(collection, name, body, done) {
  // 3-step process
  // Step 1: parse the body to separate properties and references.
  // Step 2: start transaction. apply lock, and update multiple references.
  // Step 3: Go recursive.

  // step 1
  functions._parseDocumentBody(body, function(err, res) {
    if (err) return done(err)
    // Step 2: Start transaction.
    c_name = "c`"+collection
    d_id = "d`"+collection+"`"+name
    r_id = "r`"+collection+"`"+name
    var multi = client.multi()
    // Add all properties to the document d_id
    for (var key in res.properties) {
      // TBD: only stringify if an object.
      multi.hset(d_id, key, JSON.stringify(res.properties[key]), redis.print)
    }
    // Always set _id and _collection as the document and collection id and names. Overwrite to ensure no updates.
    multi.hset(d_id, "_id", name)
    multi.hset(d_id, "_collection", collection)
    // Add all the references to the document reference r_id
    for (var key in res.references) {
      // There are two cases here.
      // case-1: res.references[key] points to an actual document. follows...
      if (typeof res.references[key] !== "string" && res.references[key] !== null) {
        r_c_name = res.references[key]._collection
        r_d_id = res.references[key]._id
        multi.hset(r_id, key, "d`"+r_c_name+"`"+r_d_id)
        multi.sadd("b`"+r_c_name+"`"+r_d_id, d_id+"`"+key) // add current doc as parent ref.
        // Now, recursively build up each reference first.
        functions.updateDocument(r_c_name, r_d_id, res.references[key], function(err, res) {
          if (err) return done(err)
        })
      }
      // case-2: res.references[key] points to a path. follows...
      else {
        // resolve path
        edgePath = functions._pathResolution(res.references[key])
        console.log(edgePath)
        functions.traverse(edgePath[0], edgePath[1], edgePath.slice(2), function(err, c_final, d_final) {
          if (err) return done(err)
          multi.hset(r_id, key, "d`"+c_final+"`"+d_final)
          multi.sadd("b`"+c_final+"`"+d_final, d_id) // add current doc as parent ref.
        })
      }

    }
    // Add the document in the collection now
    multi.zadd(c_name, Date.now(), d_id, redis.print)
    // Execute now
    multi.exec(function(err, replies) {
      if (err) return done(err)
      done(null, replies)
    })
  })
}

functions.createDocument = function createDocument(collection, name, body, done) {
  functions.updateDocument(collection, name, body, done)
}

functions.getDocument = function getDocument(collection, name, getReferences, timestamp, done) {
  client.hgetall("d`"+collection+"`"+name, function(err, res) {
    if (err) return done(err)
    done(null, res)
  })
}

functions.deleteDocument = function deleteDocument(collection, name, done) {
  // what to do here.
}

functions.traverse = function traverse(collection, name, edgePath, done) {
  // edge-case when there is no edgePath
  if (!edgePath || edgePath.length === 0) return done(null, collection, name)
  client.hget("r`"+collection+"`"+name, edgePath[0], function(err, res) {
    if (err) return done(err)
    if (res === null) return done(new Error("Path does not exist"))
    collection = res.split("`")[1]
    name = res.split("`")[2]
    if (edgePath.length == 1) {
      done(null, collection, name)
    }
    else {
      return functions.traverse(collection, name, edgePath.slice(1), done)
    }
  })
}

// Internal functions

functions._serverTime = function _serverTime() {
  var timeInstance = new Date
  return {
    "_timestamp": timeInstance.getTime(),
    "_timezoneOffset": timeInstance.getTimezoneOffset(),
    "_ISOString": timeInstance.toISOString()
  }
}

functions._getCollection = function _getCollection(name, done) {
  client.zscore("collections", name, function(err, memberTimestamp) {
    if (!memberTimestamp) {
      done(new Error("Collection name does not exist"), null)
    } else {
      done(null, {"_collection": name, "_createdAt": memberTimestamp})
    }
  })
}

functions._getBackReferences = function _getBackReferences(collection, name, done) {
  parent_id = "b`"+collection+"`"+name
  client.smembers(parent_id, function(err, res) {
    if (err) return done(err)
    // translate to collection/document syntax
    backReferences = []
    for (var key in res) {
      reference = res[key].split("`")
      backReferences.push(reference[1]+"/"+reference[2]+"/"+reference[3])
    }
    return done(null, backReferences)
  })
}

functions._parseDocumentBody = function _parseDocumentBody(body, done) {
  if (body === null || typeof body !== 'object')
    return done(new Error('Format error in processing request body, not a valid Object'))
  var res = {'properties': {}, 'references': {}}
  for (var key in body) {
    if (body.hasOwnProperty(key)) {
      // check if reference
      if (key[0] === '/') {
        rKey = key.substr(1)
        if (body[key] !== null) // only update when reference value is not null
          res['references'][rKey] = body[key]
      } else {
        if (body[key] !== null) // only update when key value is not null
          res['properties'][key] = body[key]
      }
    }
  }
  done(null, res)
}

functions._pathResolution = function _pathResolution(path) {
  // converts something like /a/b//c to ["a", "b", "c"]
  var edgePath = path.split("/")
  edgePath = edgePath.filter(function(element) {return element !== ""})
  return edgePath
}

module.exports = functions

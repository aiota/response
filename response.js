var aiota = require("aiota-utils");
var amqp = require("amqp");
var jsonValidate = require("jsonschema").validate;
var MongoClient = require("mongodb").MongoClient;

var config = null;
var processName = "response.js";

function validate(instance, schema)
{
	var v = jsonValidate(instance, schema);

	return (v.errors.length == 0 ? { isValid: true } : { isValid: false, error: v.errors });
}

function handleResponseRequest(db, msg, callback)
{
	var schema = { 
		type: "object",
		properties: {
			header: {
				type: "object",
				properties: {
					requestId: { type: "string", required: true },
					deviceId: { type: "string", required: true },
					type: { type: "string", enum: [ "ack", "nack" ], required: true },
					timestamp: { type: "integer", minimum: 0, required: true },
					ttl: { type: "integer", minimum: 0, required: true },
					encryption: {
						type: "object",
						properties: {
							method: { type: "string", required: true },
							tokencardId: { type: "string", required: true }
						},
						required: true
					}
				},
				required: true
			},
			"body": {
				type: "object",
				properties: {
					requestId: { type: "string", required: true }
				},
				required: true
			}
		}
	};

	var v = validate(msg, schema);

	if (v.isValid) {
		// Check that the message has not expired
		var now = Date.now();
		
		if ((msg.header.ttl > 0) && (now > (msg.header.timestamp + msg.header.ttl * 1000))) {
			callback({ error: "This message has expired.", errorCode: 100017 });
		}
		else {
			db.collection("actions", function(err, collection) {
				if (err) {
					callback({ error: err });
					return;
				}
	
				var progress = (msg.header.type == "ack" ? (msg.body.hasOwnProperty("progress") ? msg.body.progress : 0) : 0);
				var reason = (msg.header.type == "nack" ? (msg.body.hasOwnProperty("reason") ? msg.body.reason : "") : "");
				
				var upd = {};
				upd["$set"] = { status: (msg.header.type == "ack" ? (progress >= 100 ? 20 : 10) : 90) };
				upd["$push"] = { progress: { timestamp: msg.header.timestamp, status: (msg.header.type == "ack" ? (progress >= 100 ? "completed" : "progress " + progress + "%") : "failed") } };
	
				collection.update({ deviceId: msg.header.deviceId, "encryption.tokencardId": msg.header.encryption.tokencardId, requestId: msg.body.requestId }, upd, function(err, result) {
					callback(err ? { error: err } : { status: "OK" });
				});
			});
		}
	}
	else {
		callback({ error: v.error, errorCode: 100003 });
	}
}

var args = process.argv.slice(2);
 
MongoClient.connect("mongodb://" + args[0] + ":" + args[1] + "/" + args[2], function(err, aiotaDB) {
	if (err) {
		aiota.log(processName, "", null, err);
	}
	else {
		aiota.getConfig(aiotaDB, function(c) {
			if (c == null) {
				aiota.log(processName, "", aiotaDB, "Error getting config from database");
			}
			else {
				config = c;

				MongoClient.connect("mongodb://" + config.database.host + ":" + config.ports.mongodb + "/" + config.database.name, function(err, db) {
					if (err) {
						aiota.log(processName, config.serverName, aiotaDB, err);
					}
					else {
						var bus = amqp.createConnection(config.amqp);
						
						bus.on("ready", function() {
							var cl = { group: "response" };
							bus.queue(aiota.getQueue(cl), { autoDelete: false, durable: true }, function(queue) {
								queue.subscribe({ ack: true, prefetchCount: 1 }, function(msg) {
									handleResponseRequest(db, msg, function(result) {
										queue.shift();
									});
								});
							});
						});
		
						setInterval(function() { aiota.heartbeat(processName, config.serverName, aiotaDB); }, 10000);
		
						process.on("SIGTERM", function() {
							aiota.terminateProcess(processName, config.serverName, aiotaDB, function() {
								process.exit(1);
							});
						});
					}
				});
			}
		});
	}
});

var es = require('event-stream');
var _ = require('underscore');
var zlib = require('zlib');
var fs = require('fs');
var assert = require('assert');

var src;
if (process.argv.length > 2) {
	src = fs.createReadStream(process.argv[2]);
} else {
	src = process.stdin;
}

var schema = {
	namespace : "com.gmobi.rtb.avro",
	name : "Log",
	type : "record",
	fields : []
};

function capitalizeFirstLetter(string) {
    return string.charAt(0).toUpperCase() + string.slice(1);
}

function getPrimitiveType(v) {
	if (_.isArray(v)) {
		return "array";
	} else if (_.isObject(v)) {
		return "record";
	} else if (_.isString(v)) {
		return "string";
	} else if (_.isBoolean(v)) {
		return "boolean";
	} else if (_.isNumber(v)) {
		return "double";
	} else if (_.isNull(v)) {
		return undefined;
	}
	throw new Error("Invalid Primitive Type");
}

global.currentScope = ["com", "gmobi", "rtb", "avro"];

function addSchema(obj, schemaPart) {
	if (!_.isArray(schemaPart)) debugger;
	assert(_.isArray(schemaPart));
	_.forEach(obj, function(v, k) {
		try {
			global.currentScope.push(k);
			if (global.currentScope.length === 8) {
				if (global.currentScope[5] === "ad" & global.currentScope[6] === "info" & global.currentScope[7] === "coef")
				return undefined;
			}
			var target = _.filter(schemaPart, function(x) {
				return x.name === k;
			});
			if (_.isArray(v)) {
				if (v.length > 0) {
					var types = _.chain(v).map(getPrimitiveType).uniq().value();
					if (types.length !== 1) {
						debugger;
						throw new Error("Inconsistent array");
					}
					if (target.length === 0) {
						if (types[0] === "record") {
							schemaPart.push({
								name : k,
								type : ["null", {
									type : "array",
									items : {
										namespace : _.head(global.currentScope, global.currentScope.length - 1).join("."),
										name : capitalizeFirstLetter(k),
										type : types[0],
										fields : []
									}
								}],
								"default" : null
							});
						} else {
							schemaPart.push({
								name : k,
								type : ["null", {
									type : "array",
									items : types[0]
								}],
								"default" : null
							});
						}
						target = _.filter(schemaPart, function(x) {
							return x.name === k;
						});
					} else {
						if (target[0].type[1].type !== "array") debugger;
						assert(target[0].type[1].type === "array");
					}
					if (types[0] === "record") {
						_.forEach(v, function(v2) {
							addSchema(v2, target[0].type[1].items.fields);
						});
					}
				}
				return undefined;
			}
			if (_.isObject(v)) {
				if (!_.isString(k)) debugger;
				if (target.length === 0) {
					schemaPart.push({
						name : k,
						type : ["null", {
							namespace : _.head(global.currentScope, global.currentScope.length - 1).join("."),
							name : capitalizeFirstLetter(k),
							type : "record",
							fields : []
						}],
						"default" : null
					});
				}
				target = _.filter(schemaPart, function(x) {
					return x.name === k;
				});
				if (target.length !== 1) debugger;
				assert(target.length === 1);
				return addSchema(v,target[0].type[1].fields);
			}
			if (target.length === 0) {
				var type = getPrimitiveType(v);
				if (type) {
					schemaPart.push({
						name : k,
						type : ["null", type],
						"default" : null
					});
				}
			} else { // !schemaPart
				assert(target.length === 1);
				var type = getPrimitiveType(v);
				if (type) {
					if (target[0].type[1] !== type) debugger;
					assert(target[0].type[1] === type);
				}
			} // !schemaPart
		} finally {
			global.currentScope.pop();
		}
	});
	return undefined;
}

src.pipe(es.split())
	.pipe(es.parse())
	.pipe(es.map(function(obj, cb) {
		addSchema(obj, schema.fields);
		cb();
	}))
	.on("end", function() {
		console.log(JSON.stringify(schema, null, 2));
});

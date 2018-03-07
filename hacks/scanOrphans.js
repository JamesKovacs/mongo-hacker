// scanOrphans.js
//
// Created by Scott Kurowski on 2/28/18.
//
// useage: db.collection.scanOrphans([<force_chunk_scan=true/false>[,<user>, <pass>]])
//
// surveys sharded cluster for locations of all orphaned documents
// for hashed or unhashed shard keys
//
// this is not a supported MongoDB work, use as-is at your own risk (it's read-only, but can impact workloads)
//
// this particular hashed shard key juju (SERVER-14557 and FREE-58694) is not extensively tested but works in 3.2.15
// see also SERVER-14400, SERVER-28667 for future possibly other ways to target shards with / without hashed shard keys
// var incount = localShard.db.getSiblingDB(dbname).getCollection(collname).find({},{_id:0}).min( filter_lo ).max( filter_hi ).hint( hintfield ).explain(true).executionStats.nReturned;

DBCollection.prototype.scanOrphans = function scanOrphans(force_chunk_scan, user, pass) {
    var ns = this.toString();
    if ( ns ) { var pos = ns.indexOf("."); } else { pos = -1; }
    if ( !ns || pos < 0 ) {
        print("usage: db.collection.scanOrphans([<force_chunk_scan=true/false>[,<userid>, <password>]])");
        print("\tEither force_chunk_scan = true or a hashed shard key forces (slower) individual chunk scans.");
        return "error";
    }
    var dbname = ns.slice(0,pos);
    var collname = ns.slice(pos+1,ns.length);
    var config = db.getSiblingDB("config");
    var ns = dbname + "." + collname;
    var shards = config.shards.find().toArray().map(function(s) {
                        var str = s.host;
                        var pos = str.indexOf("/");
                        var rsname = str.slice(0,pos++);
                        var hosts = str.slice(pos,str.length);
                        //print("shard hosts are: ", hosts);
                        if ( pos == 0 ) {
                            print("shard " + s.host + " is not a replica set");
                            var URIsuffix = "/admin?maxPoolSize=1";
                        } else {
                            print("shard replica set name is: ", rsname);
                            var URIsuffix = "/admin?replicaSet=" + rsname + "&maxPoolSize=1&readPreference=primary";
                        }
                        if ( user ) { s.URI = "mongodb://" + user+":"+pass+"@" + hosts + URIsuffix; }
                        else { s.URI = "mongodb://" + hosts + URIsuffix; }
                        s.expectedDocs = 0;
                        //print("Connecting to " + s._id + ": " + s.host );
                        s.db = connect( s.URI );
                        return s;
    });
    var dbdata = config.databases.find({_id:dbname}).toArray();
    var colldata = config.collections.find({_id:dbname +"." + collname}).toArray();
    if (colldata.length > 0) {
        var shardkey = Object.keys(colldata[0].key)[0];
        var shardKeyAsField = colldata[0].key;
        var shardkeyflag = colldata[0].key[shardkey];
        var hintfield = {[shardkey]:1};
        if ( shardkeyflag == "hashed" ) {
            shardKeyAsField = shardkey + "_hashed";
            hintfield = shardKeyAsField;
            if ( !force_chunk_scan ) { print("Requiring force_chunk_scan = true because of hashed shard key"); }
        }
        print("Namespace " + ns + " shard key is { " + shardkey + " : " + shardkeyflag + " }");
        var results = shards.map(function(s) {
                                 var shardcounts = [];
                                 var mychunks = config.chunks.find({ns:dbname + "." + collname, shard:s._id}).toArray();
                                 var allchunks = config.chunks.find({ns:dbname + "." + collname}).toArray();
                                 var f_have_minKey = false;
                                 var f_have_maxKey = false;
                                 var allranges = allchunks.map(function(c) {
                                                            var filter = {};
                                                            filter[shardkey] = {};
                                                            filter[shardkey].$gte = c.min[shardkey];
                                                            filter[shardkey].$lt = c.max[shardkey];
                                                            if (c.min[shardkey] == MinKey) f_have_minKey = true;
                                                            if (c.max[shardkey] == MaxKey) f_have_maxKey = true;
                                                            return filter;
                                                            });
                                 var query_out_all = {$nor: allranges};
                                 var chunkscount = mychunks.length;
                                 if ( chunkscount > 0 && (shardkeyflag == "hashed" || force_chunk_scan) ) {
                                    print("Scanning all shards for chunks assigned to shard " + s._id + ": " + s.host );
                                    var rangecounts = mychunks.map(function(c) {
                                            var filter_lo = { [shardkey]: c.min[shardkey] };
                                            var filter_hi = { [shardkey]: c.max[shardkey] };
                                            var results = shards.map(function(S) {
                                                // this particular hashed shard key juju (SERVER-14557 and FREE-58694) is not extensively tested but works in 3.2.15
                                                // see also SERVER-14400, SERVER-28667 for future potential other ways to target shards with / without hashed shard keys
                                                var incount = S.db.getSiblingDB(dbname).getCollection(collname)
                                                                        .find({},{_id:0})
                                                                        .min( filter_lo )
                                                                        .max( filter_hi )
                                                                        .hint( hintfield )
                                                                        .explain(true).executionStats.nReturned;
                                                if ( incount > 0 ) {
                                                    if ( S._id != s._id ) {
                                                        shardcounts.push( { [S._id]: { orphanedDocs: incount, chunkLow: filter_lo._id, chunkHigh: filter_hi._id } } );
                                                        print("Shard " + s._id + " found " + incount + " docs of its chunk {" + filter_lo._id + " => " + filter_hi._id + "} in " + S._id );
                                                    } else {
                                                        //shardcounts.push( { [S._id]: { expectedDocs: incount, chunkLow: filter_lo._id, chunkHigh: filter_hi._id } } );
                                                        s.expectedDocs += incount;
                                                    }

                                                }
                                            });
                                            return results;
                                    });
                                 } else if ( chunkscount > 0 && shardkeyflag != "hashed" ) {
                                    print("Scanning all shards for chunks assigned to shard " + s._id + ": " + s.host );
                                    var ranges = mychunks.map(function(c) {
                                                              var filter = {};
                                                              filter[shardkey] = {};
                                                              filter[shardkey].$gte = c.min[shardkey];
                                                              filter[shardkey].$lt = c.max[shardkey];
                                                              return filter;
                                                            });
                                    var query_in = {$or: ranges};
                                    var query_out = {$nor: ranges};
                                    var results = shards.map(function(S) {
                                                        // ordinary query with shard key hint
                                                        var incount = S.db.getSiblingDB(dbname).getCollection(collname)
                                                                            .find(query_in,{_id:0})
                                                                            .hint( hintfield )
                                                                            .explain(true).executionStats.nReturned;
                                                        if ( incount > 0 ) {
                                                             if ( S._id != s._id ) {
                                                                shardcounts.push( { [S._id]: { orphanedDocs: incount } } );
                                                                print("Shard " + s._id + " found " + incount + " docs of its chunks in " + S._id );
                                                             } else {
                                                                //shardcounts.push( { [S._id]: { expectedDocs: incount } } );
                                                                s.expectedDocs += incount;
                                                             }
                                                        }
                                                        /*
                                                        if ( S._id == s._id ) {
                                                             // self out-count
                                                             var outcount = S.db.getSiblingDB(dbname).getCollection(collname)
                                                                            .find(query_out,{_id:0})
                                                                            .hint( {"$natural":1} )
                                                                            .explain(true).executionStats.nReturned;
                                                             if ( outcount > 0 ) {
                                                                shardcounts.push( { [S._id]: { orphanedDocs: outcount } } );
                                                                print("Shard " + s._id + " hosts " + outcount + " docs not in any of its chunks" );
                                                             }
                                                        }
                                                        */
                                                        return 1;
                                    });
                                 } else {
                                    print("Shard " + s._id + ": " + s.host + " has 0 metadata chunks, skipping shards scans.");
                                 }
                                 // global out-count for docs not belonging to ANY chunks
                                 // but only IF MinKey/MaxKey are NOT each both ranged in 1 or 2 chunks
                                 // if shard key range gaps are possible (but unsupported?), we should check for alien docs always
                                 if ( f_have_maxKey && f_have_minKey ) {
                                    print("Skipping shard " + s._id + " scan for alien orphaned docs NOT assigned to ANY chunks (shard key has both MinKey / MaxKey in chunks)");
                                 } else {
                                    print("Scanning shard " + s._id + " for alien orphaned docs NOT assigned to ANY chunks, have MinKey: " + f_have_minKey + ", have MaxKey: " + f_have_maxKey);
                                    var outcount = s.db.getSiblingDB(dbname).getCollection(collname)
                                                                            .find(query_out_all,{_id:0})
                                                                            .hint( hintfield )
                                                                            .explain(true).executionStats.nReturned;
                                    if ( outcount > 0 ) {
                                        shardcounts.push( { [s._id]: { orphanedDocs: outcount, alienToPartialShardKeyRange: true, hasMinKey: f_have_minKey, hasMaxKey: f_have_maxKey } } );
                                        print("Shard " + s._id + " hosts " + outcount + " orphaned docs alien to ALL chunks in this cluster" );
                                    }

                                 }
                                 // package the results
                                 var shardresult = {};
                                 if (s._id == dbdata[0].primary) {
                                    shardresult[s._id] = { primaryShard: true, chunks: chunkscount, expectedDocs: s.expectedDocs, orphanHostShards: shardcounts };
                                 } else {
                                    shardresult[s._id] = { chunks: chunkscount, expectedDocs: s.expectedDocs, orphanHostShards: shardcounts };
                                 }
                                 return shardresult;
        });
    } else {
        // unsharded collection on sharding-enabled db
        if (dbdata.length == 1) {
            var results = shards.map(function(s) {
                                //print("Connecting to " + s._id + ": " + s.host );
                                var sharddb = connect( s.URI );
                                var incount = sharddb.getSiblingDB(dbname).getCollection(collname).find().hint({_id:1}).explain(true).executionStats.nReturned;
                                var shardresult = {};
                                if (s._id == dbdata[0].primary) {
                                        shardresult[s._id] = {primaryShard: true, unsharded: true, expectedDocs: incount, orphanedDocs: 0 };
                                } else {
                                        shardresult[s._id] = {unsharded: true, expectedDocs: 0, orphanedDocs: incount};
                                }
                                return shardresult;
            });
        } else {
            // should never see this...
            results = {error:"Unexpected database count: " + dbdata.length + ". Is this a sharded cluster?"};
        }
    }
    return {namespace:ns, shardkey: { [shardkey]: shardkeyflag }, metadataScans:results};
};

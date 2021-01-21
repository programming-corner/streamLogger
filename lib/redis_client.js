var redis = require("redis")
const { promisify } = require('util');

const cashingConfig = {
    EXPIRECONFIGURATION: false,
    EXPIRATIONTIME: 3000,
    CHARGEEXPIRATIONDAYS: () => parseInt((+new Date) / 1000) + (86400 * 15)
}

const redisError = {
    level: "-3",
    code: "redisDB"
}

module.exports = class {

    constructor(aName, aConfig) {
        this.name = aName;
        this.client = redis.createClient(aConfig)
        var commands = ["rpush", "lpush", "BRPOP", "get", "set", "del" , "publish","subscribe","hmset"];
        for (const key of commands)
            typeof this.client[key] === "function" ? (this.client[key] = promisify(this.client[key]).bind(this.client)) : null;

        return this.client;
    }
  
    // constructor(aName, aConfig) {
    //     this.sendSocketPackage = aConfig.sendSocketPackage;
    //     delete aConfig.sendSocketPackage;
    //     this.name = aName;
    //     this.config = aConfig;
    //     this.client = redis.createClient(this.config);
    //     this.lastTimeReconn = null;
    //     this.sendTime = () => { if (!this.lastTimeReconn || ((Date.now() - this.lastTimeReconn) > 60 * 3 * 1000)) return true }

    //     var data = { msg: "node redis inst ", time: new Date(), name: this.name, redisConfig: this.config };
    //     this.client.on("error", (error) => {

    //         console.error("redis_server error on port", this.config.port);
    //         Object.assign(data, {
    //             status: 'not Connected', error, time: Date.now()
    //         });
    //         this.sendTime() && this.sendSocketPackage('redis_error', data) && (this.lastTimeReconn = Date.now());
    //     });

    //     this.client.on("connect", () => {
    //         Object.assign(data, { time: Date.now(), status: ' Connected' });
    //         this.sendSocketPackage('redis_connect', data);
    //     });

    //     //rename client
    //     this.client.client('SETNAME', this.name, (err, res) => {
    //     });

    //     this.get = promisify(this.client.get).bind(this.client);
    //     this.set = promisify(this.client.set).bind(this.client);
    //     this.rpush = promisify(this.client.rpush).bind(this.client);
    //     this.hexists = promisify(this.client.hexists).bind(this.client);
    //     this.hmset = promisify(this.client.hmset).bind(this.client);
    //     this.hmget = promisify(this.client.hmget).bind(this.client);
    //     this.hget = promisify(this.client.hget).bind(this.client);
    //     this.expire = promisify(this.client.expire).bind(this.client);
    //     this.persist = promisify(this.client.persist).bind(this.client);
    //     this.ttl = promisify(this.client.ttl).bind(this.client);
    //     this.lrange = promisify(this.client.lrange).bind(this.client);
    //     this.delete_key = promisify(this.client.del).bind(this.client);
    //     this.key_exist = promisify(this.client.exists).bind(this.client);
    //     this.key_type = promisify(this.client.type).bind(this.client);
    //     this.hdelkey = promisify(this.client.hdel).bind(this.client);
    //     this.hsetnx = promisify(this.client.HSETNX).bind(this.client);
    //     this.hincby = promisify(this.client.hincrby).bind(this.client);
    //     this.sadd = promisify(this.client.SADD).bind(this.client);
    //     this.srem = promisify(this.client.SREM).bind(this.client);
    //     this.SMEMBERS = promisify(this.client.SMEMBERS).bind(this.client);
    //     this.hgetall = promisify(this.client.hgetall).bind(this.client);

    // }

    // async getByID(ctx, id) {
    //     try {
    //         return await this.get(id);
    //     } catch (e) {
    //         ctx.setError(redisError.level, redisError.code, e.message || e.stack)
    //     }
    // }

    // async hashEntryExists(ctx, hashname, id) {
    //     try {
    //         return await this.hexists(hashname, id);
    //     } catch (e) {
    //         ctx.setError(redisError.level, redisError.code, e.stack)
    //     }
    // }

    // async getHashEntry(ctx, hashname, id) {
    //     try {
    //         return await this.hget(hashname, id);
    //     } catch (e) {
    //         ctx.setError(redisError.level, redisError.code, e.stack)
    //     }
    // }

    // async delHashEntry(ctx, hashname, key) {
    //     try {
    //         console.log('[', new Date(new Date() + 'UTC'), ']', "delHashEntry", hashname, key)
    //         return await this.hdelkey(hashname, key);
    //     } catch (e) {
    //         ctx.setError(redisError.level, redisError.code, e.stack)
    //     }
    // }

    // async getSerialNumber(ctx, hashname) {
    //     try {
    //         var today = (new Date(new Date() + 'UTC').toISOString().slice(0, 10));
    //         var multRes = await new Promise(resolve =>
    //             this.client.multi().hsetnx(hashname, "Date", today).hsetnx(hashname, "SN", 1).hsetnx(hashname, "LastVTrx", "[]")
    //                 .hmget(hashname, ["Date", "SN", "LastVTrx"]).exec((err, replies) => {
    //                     if (err) return reject(err)
    //                     return resolve(replies);
    //                 }));

    //         if (multRes[3][0].toString() != today.toString())
    //             return await this.setHashentryAllInOne(ctx, hashname, ["Date", today, "SN", 1]) && [today, "1", multRes[3][2]]

    //         return multRes[3];
    //     } catch (e) {
    //         ctx.setError(redisError.level, redisError.code, e.stack)
    //     }
    // }

    // async getMHashEntry(ctx, hashname, fields) {
    //     try {
    //         return await this.hmget(hashname, fields);
    //     } catch (e) {
    //         ctx.setError(redisError.level, redisError.code, e.stack)
    //     }

    // }

    // async setHashentry(ctx, hashname, id, status) {
    //     try {
    //         var res = await this.hmset([hashname, id, status]);
    //     } catch (e) {
    //         ctx.setError(redisError.level, redisError.code, e.stack)
    //     }

    //     return res
    // }

    // async setHashentryAllInOne(ctx, hashname, allKeysSndValues) {
    //     try {
    //         var res = await this.hmset(hashname, allKeysSndValues);
    //     } catch (e) {
    //         ctx.setError(redisError.level, redisError.code, e.stack)
    //     }
    //     return res
    // }

    // async setExpiredHashentryAllInOne(ctx, hashname, allKeysSndValues) {
    //     try {
    //         var multRes = await new Promise((resolve, reject) =>
    //             this.client.multi().hmset(hashname, allKeysSndValues).expireat(hashname, cashingConfig.CHARGEEXPIRATIONDAYS()).exec((err, replies) => {
    //                 if (err) return reject(err)
    //                 return resolve(replies);
    //             }));
    //         return multRes[1];

    //     } catch (e) {
    //         return ctx.setError(redisError.level, redisError.code, e.stack)
    //     }
    // }


    // async cleanCashedData(ctx, hashname, fieldsToGet) {
    //     try {
    //         var multRes = await new Promise((resolve, reject) =>
    //             this.client.multi().hmget(hashname, fieldsToGet).del(hashname).exec((err, replies) => {
    //                 if (err) return reject(err)
    //                 return resolve(replies);
    //             }));

    //         return multRes[0];

    //     } catch (e) {
    //         return ctx.setError(redisError.level, redisError.code, e.stack)
    //     }
    // }

    // async hashGetAll(ctx, hashname) {
    //     try {
    //         return await this.hgetall(hashname);
    //     } catch (e) {
    //         ctx.setError(redisError.level, redisError.code, e.stack)
    //     }
    // }

    // async deleteKey(ctx, keyName) {
    //     try {
    //         return await this.delete_key(keyName);
    //     } catch (e) {
    //         ctx.setError(redisError.level, redisError.code, e.stack)
    //     }

    // }

    // async setKey(ctx, keyName, keyValue) {
    //     try {
    //         return await this.set(keyName, keyValue);
    //     } catch (e) {
    //         ctx.setError(redisError.level, redisError.code, e.stack)
    //     }

    // }

    // async keyExist(ctx, keyName) {
    //     try {
    //         return await this.key_exist(keyName);
    //     } catch (e) {
    //         ctx.setError(redisError.level, redisError.code, e.message || e.stack)
    //     }
    // }

    // async keyType(ctx, keyName) {
    //     try {
    //         return await this.key_type(keyName);
    //     } catch (e) {
    //         ctx.setError(redisError.level, redisError.code, e.stack)
    //     }
    // }

    // async setkeyExpire(ctx, keyName) {

    //     if (!cashingConfig.EXPIRECONFIGURATION) return
    //     try {
    //         return await this.expire(keyName, cashingConfig.EXPIRATIONTIME);
    //     } catch (e) {
    //         ctx.setError(redisError.level, redisError.code, e.stack)
    //     }
    // }

    // async setkeyPersist(ctx, keyName) {
    //     if (!cashingConfig.EXPIRECONFIGURATION) return
    //     try {
    //         return await this.persist(keyName);
    //     } catch (e) {
    //         ctx.setError(redisError.level, redisError.code, e.stack)
    //     }
    // }

    // async getkeyTTL(ctx, keyName) {
    //     if (!cashingConfig.EXPIRECONFIGURATION) return
    //     try {
    //         return await this.ttl(keyName);
    //     } catch (e) {
    //         ctx.setError(redisError.level, redisError.code, e.stack)
    //     }
    // }

    // async hincBy(ctx, keyName, filed, value) {
    //     try {
    //         return await this.hincby(keyName, filed, value);
    //     } catch (e) {
    //         ctx.setError(redisError.level, redisError.code, e.stack)
    //     }
    // }

    // async hincAppend(ctx, hashname, value, value2) {
    //     try {
    //         var multRes = await new Promise((resolve, reject) =>
    //             this.client.multi().hset(hashname, "LastVTrx", value2).hincrby(hashname, "SN", value).exec((err, replies) => {
    //                 if (err) return reject(err)
    //                 return resolve(replies);
    //             }));
    //         return multRes[1];

    //     } catch (e) {
    //         ctx.setError(redisError.level, redisError.code, e.stack)
    //     }
    // }

    // async addToQueue(ctx, queueName, data) {
    //     try {
    //         return await this.rpush(queueName, data);
    //     } catch (e) {
    //         ctx.setError(redisError.level, redisError.code, e.stack)
    //     }
    // }

    // async addToSet(ctx, setName, data) {
    //     try {
    //         return await this.sadd(setName, data);
    //     } catch (e) {
    //         ctx.setError(redisError.level, redisError.code, e.stack)
    //     }
    // }

    // async removeFromSet(ctx, setName, data) {
    //     try {
    //         return await this.srem(setName, data);
    //     } catch (e) {
    //         ctx.setError(redisError.level, redisError.code, e.stack)
    //     }
    // }

    // async getsetMembers(ctx, setName) {
    //     try {
    //         return await this.SMEMBERS(setName);
    //     } catch (e) {
    //         ctx.setError(redisError.level, redisError.code, e.stack)
    //     }
    // }

    // async checkServiceData(ctx, ser_id, serviceRpoHash, stopedServicesSet, stopedProviderSet) {
    //     try {
    //         return await new Promise((resolve, reject) =>
    //             this.client.multi().hexists(stopedServicesSet, ser_id).hget(serviceRpoHash, `ser_${ser_id}`)
    //                 .hkeys(stopedProviderSet).exec((err, replies) => {
    //                     if (err) {
    //                         ctx.setError(redisError.level, redisError.code, err)
    //                         return reject(err)
    //                     }
    //                     return resolve(replies);
    //                 }));
    //     } catch (e) {
    //         ctx.setError(redisError.level, redisError.code, e.stack)
    //     }
    // }

    async addToStream(streamName, entryId, streamData, mapingKey) {
        try {
            await new Promise((resolve, reject) =>
                this.client.multi().xadd(streamName, entryId, ...streamData).exec((err, replies) => {
                    if (err)
                        return reject(err)
                    return resolve(replies);
                }));
        } catch (e) {
            console.error("addToStream >> some errors happen,", e)
        }
    }
}
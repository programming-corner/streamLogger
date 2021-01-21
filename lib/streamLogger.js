const dms_cash = require('./redis_client');
const streamConfig = require('../configs/systemVaraible/streamsData');

function isValidType(data, target) {
    return (target !== 'array') ?
        typeof data === target : Array.isArray(data)
}

function handleStream(streamData) {
    var finalStreamData = []
    for (var index = 1; index < streamData.length; index += 2)
        if (streamData[index]) finalStreamData.push(streamData[index - 1], streamData[index])
    return finalStreamData;
}

module.exports = class {

    constructor(aNode, aConfig) {
        this.client = new dms_cash("streamLogger", { ...aConfig, sendSocketPackage: aNode.sendSocketPackage });
        aNode = null;
    }

    async accumulateRequestStream(redisClient, accmulatorId, streamConfig, streamData) {
        //TODO must be wrapped with try and catch ?!!
        let accmulatorName = `logger:${accmulatorId}`;
        if (!streamData && !streamConfig) return accmulatorName;

        if (streamConfig && !isValidType(streamConfig, 'object'))
            console.error(accmulatorName, " : not valid stream config");
        if (streamData && !isValidType(streamData, 'array'))
            console.error(accmulatorName, " : not valid stream Data");

        let streamContent = [...(streamConfig ? Object.entries(streamConfig).flat() : []),
        ...(streamData ? streamData : [])];

        streamContent = handleStream(streamContent)
        if (streamContent.length)
            await redisClient.hmset(accmulatorName, streamContent);

        return accmulatorName;
    }

    replaceStremKey(reqLoggerData) {
        if (!reqLoggerData["$data"]) return false;
        reqLoggerData.streamData.push(reqLoggerData.replace, reqLoggerData["$data"]);
        return true;
    }

    validateStream(reqLoggerData) {
        let { streamName, replace } = reqLoggerData;
        let BPstreamConfig = streamConfig[streamName];
        if (!BPstreamConfig) return false;

        let { keys: streamKeys, isMandatory } = BPstreamConfig;
        if (!streamKeys) return false;
        reqLoggerData.streamData = [];

        if (replace) return this.replaceStremKey(reqLoggerData); //check replacement;

        //validateKeys
        for (let key of streamKeys) {
            if (!reqLoggerData[key] && isMandatory)//TODO: null in accounting is string
                return false;
            else if (reqLoggerData[key]) reqLoggerData.streamData.push(key, reqLoggerData[key]);
        }
        return reqLoggerData.streamData.length ? true : false
    }

    async publishStream(reqLoggerData) {
        let isValid = this.validateStream(reqLoggerData);
        if (!isValid) return false;

        let { streamName, streamKey, streamData, Date: streamDate } = reqLoggerData;
        let date = streamDate ? streamDate.split(' ')[0] : new Date(new Date() + "UTC").toISOString().split('T')[0];
        streamName = `${streamName}_${date.split('-').reverse().join('')}`;

        await this.client.addToStream(streamName, '*', streamData, reqLoggerData[streamKey]);
    }
}

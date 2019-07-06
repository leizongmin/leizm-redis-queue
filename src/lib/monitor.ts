/**
 * super-queue Monitor
 *
 * @authro Zongmin Lei <leizongmin@gmail.com>
 */

import { EventEmitter } from "events";
import * as utils from "./utils";
import { RedisOptions, Redis } from "ioredis";

interface ConsumerInfo {
  name: string;
  startedAt: number;
  msgTotal: number;
  msgSuccess: number;
  msgError: number;
  msgExpired: number;
  capacity: number;
  processingCount: number;
  uptime?: number;
}

// content: startedAt,msgTotal,msgSuccess,msgError,msgExpired,capacity,processingCount
function parseConsumerInfo(name: string, content: string): ConsumerInfo {
  const info = utils.splitString(content, ",", 7);
  const ret = {
    name,
    startedAt: Number(info[0]),
    msgTotal: Number(info[1]),
    msgSuccess: Number(info[2]),
    msgError: Number(info[3]),
    msgExpired: Number(info[4]),
    capacity: Number(info[5]),
    processingCount: Number(info[6]),
  };
  return ret;
}

interface ProducerInfo {
  name: string;
  startedAt: number;
  msgTotal: number;
  msgSuccess: number;
  msgError: number;
  msgExpired: number;
  uptime?: number;
}

// content: startedAt,msgTotal,msgSuccess,msgError,msgExpired
function parseProducerInfo(name: string, content: string): ProducerInfo {
  const info = utils.splitString(content, ",", 5);
  const ret = {
    name,
    startedAt: Number(info[0]),
    msgTotal: Number(info[1]),
    msgSuccess: Number(info[2]),
    msgError: Number(info[3]),
    msgExpired: Number(info[4]),
  };
  return ret;
}

function computeOnlineOfflineList(previous, current) {
  const inArray = (arr, a) => {
    for (const b of arr) {
      if (a.queue === b.queue && a.name === b.name) {
        return true;
      }
    }
    return false;
  };
  const arrayStrip = (arrA, arrB) => {
    return arrA.filter(a => !inArray(arrB, a));
  };
  const online = arrayStrip(current, previous);
  const offline = arrayStrip(previous, current);
  return { online, offline };
}

export interface MonitorOptions {
  interval: number;
  redis: RedisOptions;
}

export class Monitor extends EventEmitter {
  private readonly _redis: Redis;
  public readonly name: string;
  private readonly _redisPrefix: string;
  private readonly _fullRedisPrefix: string;
  private readonly _interval: number;
  private _exited: boolean;
  private readonly _startedAt: number;
  private readonly _lastChecked: number;
  private readonly _lastCheckNotified: boolean;
  private readonly _producers: string[];
  private readonly _previousProducers: string[];
  private readonly _consumers: string[];
  private readonly _previousConsumers: string[];
  private readonly _autoCheckTid: NodeJS.Timeout;
  private _emitCounter: number;
  private readonly _debug: (...args: any) => void;

  /**
   * Constructor
   *
   * @param {Object} options
   *   - {Number} interval
   *   - {Object} redis
   *     - {String} host
   *     - {Number} port
   *     - {Number} db
   *     - {String} prefix
   */
  constructor(options: MonitorOptions) {
    super();

    options = Object.assign({}, options || {});

    this._redis = utils.createRedisClient(options.redis);
    this.name = utils.generateClientId("monitor");
    this._redisPrefix = (options.redis && options.redis.prefix) || "";
    this._fullRedisPrefix =
      options.redis && options.redis.keyPrefix ? options.redis.keyPrefix + this._redisPrefix : this._redisPrefix;

    this._interval = options.interval || 2;

    this._exited = false;
    this._startedAt = utils.secondTimestamp();

    this._lastChecked = utils.secondTimestamp();
    this._lastCheckNotified = false;
    this._producers = [];
    this._previousProducers = [];
    this._consumers = [];
    this._previousConsumers = [];
    this._autoCheckTid = setInterval(() => this.check(), this._interval * 1000);
    this._emitCounter = 0;
    this.on("consumerDown", info => {
      this.withdrawnProcessingQueue(info);
    });
    this.check();

    this._debug = utils.debug("monitor:" + this.name);
    this._debug("created: interval=%s, redis=%j", this._interval, options.redis);
  }

  /**
   * check client status
   *
   * @param {Function} callback
   */
  check(callback) {
    if (this._exited) return;

    if (utils.secondTimestamp() - this._lastChecked < this._interval) {
      return callback && callback(null, this._getCheckResult());
    }

    const heartbeatKey = utils.getHeartbeatKey(this._fullRedisPrefix, "*");
    this._debug("checking: %s", heartbeatKey);
    this._redis.keys(heartbeatKey, (err, keys) => {
      if (err) return callback(err);

      const producers = [];
      const consumers = [];

      for (let i = 0; i < keys.length; i++) {
        const s = keys[i].split(":").slice(-3);
        const type = s[0];
        const queue = s[1];
        const name = s[2];
        if (type === "C") {
          consumers.push({ queue, name });
        } else if (type === "P") {
          producers.push({ queue, name });
        }
      }

      const emit = (event, list) => {
        if (this._emitCounter < 1) return this._debug("fist time emit, skip");
        for (const item of list) {
          this.emit(event, item);
        }
      };

      this._previousProducers = this._producers;
      this._producers = producers;
      this._previousConsumers = this._consumers;
      this._consumers = consumers;
      this._lastChecked = utils.secondTimestamp();

      const producersInfo = computeOnlineOfflineList(this._previousProducers, this._producers);
      this._producersOnline = producersInfo.online;
      this._producersOffline = producersInfo.offline;
      emit("producerUp", this._producersOnline);
      emit("producerDown", this._producersOffline);

      const consumersInfo = computeOnlineOfflineList(this._previousConsumers, this._consumers);
      this._consumersOnline = consumersInfo.online;
      this._consumersOffline = consumersInfo.offline;
      emit("consumerUp", this._consumersOnline);
      emit("consumerDown", this._consumersOffline);

      this._emitCounter += 1;

      return callback && callback(null, this._getCheckResult());
    });
  }

  _getCheckResult() {
    function getArray(arr) {
      return Array.isArray(arr) ? arr : [];
    }

    return {
      consumer: {
        online: getArray(this._consumersOnline).slice(),
        offline: getArray(this._consumersOffline).slice(),
      },
      producer: {
        online: getArray(this._producersOnline).slice(),
        offline: getArray(this._producersOffline).slice(),
      },
    };
  }

  /**
   * get client status
   *
   * @param {Function} callback
   */
  public clientStatus(
    callback: (
      err: Error | null,
      ret?: {
        producers: ProducerInfo[];
        consumers: ConsumerInfo[];
      },
    ) => void,
  ) {
    const heartbeatKey = utils.getHeartbeatKey(this._fullRedisPrefix, "*");
    this._redis.keys(heartbeatKey, (err, keys) => {
      if (err) return callback(err);

      if (keys.length < 1) {
        return callback(null, { producers: [], consumers: [] });
      }

      this._redis.mget(...keys, (err: Error | null, values: string[]) => {
        if (err) return callback(err);

        const producers = [];
        const consumers = [];

        for (let i = 0; i < keys.length; i++) {
          if (values[i]) {
            const s = keys[i].split(":").slice(-3);
            if (s[0] === "C") {
              consumers.push(parseConsumerInfo(s[1], values[i]));
            } else if (s[0] === "P") {
              producers.push(parseProducerInfo(s[1], values[i]));
            }
          }
        }

        const timestamp = utils.secondTimestamp();
        for (const item of producers) {
          item.uptime = timestamp - item.startedAt;
        }
        for (const item of consumers) {
          item.uptime = timestamp - item.startedAt;
        }

        callback(null, { producers, consumers });
      });
    });
  }

  /**
   * get queue status
   *
   * @param {Function} callback
   */
  public queueStatus(callback: (err: Error | null, list?: Array<{ name: string; length: number }>) => void) {
    const queueKey = utils.getQueueKey(this._fullRedisPrefix, "*");
    this._redis.keys(queueKey, (err, keys) => {
      if (err) return callback(err);

      const q = this._redis.multi();
      for (const k of keys) {
        q.llen(k);
      }
      q.exec((err, rets) => {
        if (err) return callback(err);

        const list = [];
        for (let i = 0; i < keys.length; i++) {
          const name = keys[i].split(":").pop()!;
          const length = Number(rets[i][1]);
          list.push({ name, length });
        }

        callback(null, list);
      });
    });
  }

  /**
   * withdrawn processing queue
   *
   * @param {Object} info
   *   - {Stirng} queue
   *   - {String} name
   * @param {Function} callback
   */
  public withdrawnProcessingQueue(info: { queue: string; name: string }, callback: (err: Error | null) => void) {
    const processingKey = utils.getProcessingQueueKey(this._redisPrefix, info.queue, info.name);
    const waitingKey = utils.getQueueKey(this._redisPrefix, info.queue);
    this._debug("withdrawnProcessingQueue: %s -> %s", processingKey, waitingKey);

    callback = callback || function() {};
    const next = (err: Error | null, ret: any) => {
      if (err) return callback(err);
      if (!ret) return callback(null);
      if (ret !== true) this._debug("withdrawnProcessingQueue: %s -> %s %s", processingKey, waitingKey, ret);
      this._redis.rpoplpush(processingKey, waitingKey, next);
    };
    next(null, true);
  }

  /**
   * exit
   *
   * @param {Function} callback
   */
  public exit(callback: (err: Error | null) => void) {
    this._exited = true;
    this._redis.disconnect();
    callback && callback(null);
  }
}

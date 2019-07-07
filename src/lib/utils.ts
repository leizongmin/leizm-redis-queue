/**
 * super-queue utils
 *
 * @authro Zongmin Lei <leizongmin@gmail.com>
 */

import * as crypto from "crypto";
import * as createDebug from "debug";
import * as Redis from "ioredis";

/**
 * Create Debug Function
 *
 * @param {String} name
 * @return {Function}
 */
export function debug(name: string) {
  return createDebug("super-queue:" + name);
}

/**
 * Generate Redis Client
 *
 * @param {Object} options
 * @return {Object}
 */
export function createRedisClient(options: RedisOptions) {
  return new Redis(options);
}

export interface RedisOptions extends Redis.RedisOptions {
  prefix?: string;
}

/**
 * Returns md5
 *
 * @param {String} text
 */
export function md5(text: string) {
  return crypto
    .createHash("md5")
    .update(text)
    .digest("hex");
}

/**
 * Generate Client ID
 *
 * @param {String} type should be one of "producer", "consumer", "monitor"
 * @return {String}
 */
export function generateClientId(type: string) {
  return type.slice(0, 1).toUpperCase() + md5(Date.now() + "" + Math.random()).slice(0, 9);
}

/**
 * Get Second Timestamp
 *
 * @return {Number}
 */
export function secondTimestamp() {
  return (Date.now() / 1000) << 0;
}

/**
 * Integer Value to Base64 String
 *
 * @param {Number} value
 * @return {String}
 */
export function integerToShortString(value: number) {
  return Number(value).toString(36);
}

/**
 * Split String by Separator
 *
 * @param {String} text
 * @param {String} separator
 * @param {Number} length
 * @return {Array}
 */
export function splitString(text: string, separator: string, length: number) {
  const list = [];
  let lastIndex = 0;
  for (let i = 0; i < length - 1; i++) {
    const j = text.indexOf(separator, lastIndex);
    if (j === -1) {
      break;
    } else {
      list.push(text.slice(lastIndex, j));
      lastIndex = j + 1;
    }
  }
  list.push(text.slice(lastIndex, text.length));
  return list;
}

export function getQueueKey(prefix: string, queue: string) {
  return prefix + "Q:" + queue;
}

export function getProcessingQueueKey(prefix: string, queue: string, name: string) {
  return prefix + "PQ:" + queue + (name ? ":" + name : "");
}

export function getCallbackKey(prefix: string, queue: string, name: string) {
  return prefix + "CB:" + queue + (name ? ":" + name : "");
}

export function getHeartbeatKey(prefix: string, type: string, queue?: string, name?: string) {
  return prefix + "H:" + type.slice(0, 1).toUpperCase() + (queue ? ":" + queue : "") + (name ? ":" + name : "");
}

/**
 * 消息已过期（未被处理）
 */
export class MessageExpiredError extends Error {
  public code: string;
  constructor(message?: string) {
    super(message);
    Error.captureStackTrace(this, MessageExpiredError);
    this.name = "MessageExpiredError";
    this.message = message || "";
    this.code = "msg_expired";
  }
}

/**
 * 消息已过期（处理超时）
 */
export class MessageProcessingTimeoutError extends Error {
  public code: string;
  constructor(message?: string) {
    super(message);
    Error.captureStackTrace(this, MessageProcessingTimeoutError);
    this.name = "MessageProcessingTimeoutError";
    this.message = message || "";
    this.code = "msg_timeout";
  }
}

export function checkQueueName(name: string) {
  if (name.indexOf(":") !== -1) throw new Error(`invalid queue name "${name}", cannot contains colon ":"`);
}

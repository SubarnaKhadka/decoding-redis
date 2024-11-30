"use strict";

const Buffer = require("buffer").Buffer;
const StringDecoder = require("string_decoder").StringDecoder;
const decoder = new StringDecoder();
const errors = require("redis-errors");
const ReplyError = errors.ReplyError;
const ParserError = errors.ParserError;
var bufferPool = Buffer.allocUnsafe(32 * 1024);
var bufferOffset = 0;
var interval = null;
var counter = 0;
var notDecreased = 0;

function parseSimpleNumbers(parser) {
  const length = parser.buffer.length - 1;
  var offset = parser.offset;
  var number = 0;
  var sign = 1;

  if (parser.buffer[offset] === 45) {
    sign = -1;
    offset++;
  }

  while (offset < length) {
    const c1 = parser.buffer[offset++];
    if (c1 === 13) {
      // \r\n
      parser.offset = offset + 1;
      return sign * number;
    }
    number = number * 10 + (c1 - 48);
  }
}

function parseStringNumbers(parser) {
  const length = parser.buffer.length - 1;
  var offset = parser.offset;
  var number = 0;
  var res = "";

  if (parser.buffer[offset] === 45) {
    res += "-";
    offset++;
  }

  while (offset < length) {
    var c1 = parser.buffer[offset++];
    if (c1 === 13) {
      // \r\n
      parser.offset = offset + 1;
      if (number !== 0) {
        res += number;
      }
      return res;
    } else if (number > 429496728) {
      res += number * 10 + (c1 - 48);
      number = 0;
    } else if (c1 === 48 && number === 0) {
      res += 0;
    } else {
      number = number * 10 + (c1 - 48);
    }
  }
}

function parseSimpleString(parser) {
  const start = parser.offset;
  const buffer = parser.buffer;
  const length = buffer.length - 1;
  var offset = start;

  while (offset < length) {
    if (buffer[offset++] === 13) {
      // \r\n
      parser.offset = offset + 1;
      if (parser.optionReturnBuffers === true) {
        return parser.buffer.slice(start, offset - 1);
      }
      return parser.buffer.toString("utf8", start, offset - 1);
    }
  }
}

function parseLength(parser) {
  const length = parser.buffer.length - 1;
  var offset = parser.offset;
  var number = 0;

  while (offset < length) {
    const c1 = parser.buffer[offset++];
    if (c1 === 13) {
      parser.offset = offset + 1;
      return number;
    }
    number = number * 10 + (c1 - 48);
  }
}

function parseInteger(parser) {
  if (parser.optionStringNumbers === true) {
    return parseStringNumbers(parser);
  }
  return parseSimpleNumbers(parser);
}

function parseBulkString(parser) {
  const length = parseLength(parser);
  if (length === undefined) {
    return;
  }
  if (length < 0) {
    return null;
  }
  const offset = parser.offset + length;
  if (offset + 2 > parser.buffer.length) {
    parser.bigStrSize = offset + 2;
    parser.totalChunkSize = parser.buffer.length;
    parser.bufferCache.push(parser.buffer);
    return;
  }
  const start = parser.offset;
  parser.offset = offset + 2;
  if (parser.optionReturnBuffers === true) {
    return parser.buffer.slice(start, offset);
  }
  return parser.buffer.toString("utf8", start, offset);
}

function parseError(parser) {
  var string = parseSimpleString(parser);
  if (string !== undefined) {
    if (parser.optionReturnBuffers === true) {
      string = string.toString();
    }
    return new ReplyError(string);
  }
}

function handleError(parser, type) {
  const err = new ParserError(
    "Protocol error, got " +
      JSON.stringify(String.fromCharCode(type)) +
      " as reply type byte",
    JSON.stringify(parser.buffer),
    parser.offset
  );
  parser.buffer = null;
  parser.returnFatalError(err);
}

function parseArray(parser) {
  const length = parseLength(parser);
  if (length === undefined) {
    return;
  }
  if (length < 0) {
    return null;
  }
  const responses = new Array(length);
  return parseArrayElements(parser, responses, 0);
}

function pushArrayCache(parser, array, pos) {
  parser.arrayCache.push(array);
  parser.arrayPos.push(pos);
}

function parseArrayChunks(parser) {
  var arr = parser.arrayCache.pop();
  var pos = parser.arrayPos.pop();
  if (parser.arrayCache.length) {
    const res = parseArrayChunks(parser);
    if (res === undefined) {
      pushArrayCache(parser, arr, pos);
      return;
    }
    arr[pos++] = res;
  }
  return parseArrayElements(parser, arr, pos);
}

function parseArrayElements(parser, responses, i) {
  const bufferLength = parser.buffer.length;
  while (i < responses.length) {
    const offset = parser.offset;
    if (parser.offset >= bufferLength) {
      pushArrayCache(parser, responses, i);
      return;
    }
    const response = parseType(parser, parser.buffer[parser.offset++]);
    if (response === undefined) {
      if (!(parser.arrayCache.length || parser.bufferCache.length)) {
        parser.offset = offset;
      }
      pushArrayCache(parser, responses, i);
      return;
    }
    responses[i] = response;
    i++;
  }

  return responses;
}

function parseType(parser, type) {
  switch (type) {
    case 36:
      return parseBulkString(parser);
    case 43:
      return parseSimpleString(parser);
    case 42:
      return parseArray(parser);
    case 58:
      return parseInteger(parser);
    case 45:
      return parseError(parser);
    default:
      return handleError(parser, type);
  }
}

function decreaseBufferPool() {
  if (bufferPool.length > 50 * 1024) {
    if (counter === 1 || notDecreased > counter * 2) {
      const minSliceLen = Math.floor(bufferPool.length / 10);
      const sliceLength =
        minSliceLen < bufferOffset ? bufferOffset : minSliceLen;
      bufferOffset = 0;
      bufferPool = bufferPool.slice(sliceLength, bufferPool.length);
    } else {
      notDecreased++;
      counter--;
    }
  } else {
    clearInterval(interval);
    counter = 0;
    notDecreased = 0;
    interval = null;
  }
}

function resizeBuffer(length) {
  if (bufferPool.length < length + bufferOffset) {
    const multiplier = length > 1024 * 1024 * 75 ? 2 : 3;
    if (bufferOffset > 1024 * 1024 * 111) {
      bufferOffset = 1024 * 1024 * 50;
    }
    bufferPool = Buffer.allocUnsafe(length * multiplier + bufferOffset);
    bufferOffset = 0;
    counter++;
    if (interval === null) {
      interval = setInterval(decreaseBufferPool, 50);
    }
  }
}

function concatBulkString(parser) {
  const list = parser.bufferCache;
  const oldOffset = parser.offset;
  var chunks = list.length;
  var offset = parser.bigStrSize - parser.totalChunkSize;
  parser.offset = offset;
  if (offset <= 2) {
    if (chunks === 2) {
      return list[0].toString("utf8", oldOffset, list[0].length + offset - 2);
    }
    chunks--;
    offset = list[list.length - 2].length + offset;
  }
  var res = decoder.write(list[0].slice(oldOffset));
  for (var i = 1; i < chunks - 1; i++) {
    res += decoder.write(list[i]);
  }
  res += decoder.end(list[i].slice(0, offset - 2));
  return res;
}

function concatBulkBuffer(parser) {
  const list = parser.bufferCache;
  const oldOffset = parser.offset;
  const length = parser.bigStrSize - oldOffset - 2;
  var chunks = list.length;
  var offset = parser.bigStrSize - parser.totalChunkSize;
  parser.offset = offset;
  if (offset <= 2) {
    if (chunks === 2) {
      return list[0].slice(oldOffset, list[0].length + offset - 2);
    }
    chunks--;
    offset = list[list.length - 2].length + offset;
  }
  resizeBuffer(length);
  const start = bufferOffset;
  list[0].copy(bufferPool, start, oldOffset, list[0].length);
  bufferOffset += list[0].length - oldOffset;
  for (var i = 1; i < chunks - 1; i++) {
    list[i].copy(bufferPool, bufferOffset);
    bufferOffset += list[i].length;
  }
  list[i].copy(bufferPool, bufferOffset, 0, offset - 2);
  bufferOffset += offset - 2;
  return bufferPool.slice(start, bufferOffset);
}

class JavascriptRedisParser {
  /**
   * Javascript Redis Parser constructor
   * @param {{returnError: Function, returnReply: Function, returnFatalError?: Function, returnBuffers: boolean, stringNumbers: boolean }} options
   * @constructor
   */
  constructor(options) {
    if (!options) {
      throw new TypeError("Options are mandatory.");
    }
    if (
      typeof options.returnError !== "function" ||
      typeof options.returnReply !== "function"
    ) {
      throw new TypeError(
        "The returnReply and returnError options have to be functions."
      );
    }
    this.setReturnBuffers(!!options.returnBuffers);
    this.setStringNumbers(!!options.stringNumbers);
    this.returnError = options.returnError;
    this.returnFatalError = options.returnFatalError || options.returnError;
    this.returnReply = options.returnReply;
    this.reset();
  }

  reset() {
    this.offset = 0;
    this.buffer = null;
    this.bigStrSize = 0;
    this.totalChunkSize = 0;
    this.bufferCache = [];
    this.arrayCache = [];
    this.arrayPos = [];
  }

  setReturnBuffers(returnBuffers) {
    if (typeof returnBuffers !== "boolean") {
      throw new TypeError("The returnBuffers argument has to be a boolean");
    }
    this.optionReturnBuffers = returnBuffers;
  }

  setStringNumbers(stringNumbers) {
    if (typeof stringNumbers !== "boolean") {
      throw new TypeError("The stringNumbers argument has to be a boolean");
    }
    this.optionStringNumbers = stringNumbers;
  }

  execute(buffer) {
    if (this.buffer === null) {
      this.buffer = buffer;
      this.offset = 0;
    } else if (this.bigStrSize === 0) {
      const oldLength = this.buffer.length;
      const remainingLength = oldLength - this.offset;
      const newBuffer = Buffer.allocUnsafe(remainingLength + buffer.length);
      this.buffer.copy(newBuffer, 0, this.offset, oldLength);
      buffer.copy(newBuffer, remainingLength, 0, buffer.length);
      this.buffer = newBuffer;
      this.offset = 0;
      if (this.arrayCache.length) {
        const arr = parseArrayChunks(this);
        if (arr === undefined) {
          return;
        }
        this.returnReply(arr);
      }
    } else if (this.totalChunkSize + buffer.length >= this.bigStrSize) {
      this.bufferCache.push(buffer);
      var tmp = this.optionReturnBuffers
        ? concatBulkBuffer(this)
        : concatBulkString(this);
      this.bigStrSize = 0;
      this.bufferCache = [];
      this.buffer = buffer;
      if (this.arrayCache.length) {
        this.arrayCache[0][this.arrayPos[0]++] = tmp;
        tmp = parseArrayChunks(this);
        if (tmp === undefined) {
          return;
        }
      }
      this.returnReply(tmp);
    } else {
      this.bufferCache.push(buffer);
      this.totalChunkSize += buffer.length;
      return;
    }

    while (this.offset < this.buffer.length) {
      const offset = this.offset;
      const type = this.buffer[this.offset++];
      const response = parseType(this, type);
      if (response === undefined) {
        if (!(this.arrayCache.length || this.bufferCache.length)) {
          this.offset = offset;
        }
        return;
      }

      if (type === 45) {
        this.returnError(response);
      } else {
        this.returnReply(response);
      }
    }

    this.buffer = null;
  }
}

module.exports = JavascriptRedisParser;

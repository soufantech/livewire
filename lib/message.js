const uuid = require('uuid/v4');
const { DemuxError } = require('./error');

class Message {
  constructor({
    value,
    key,
    partition,
    topic,
    headers = {},
    messageId = uuid(),
    contextId = uuid(),
  }) {
    this.value = value;
    this.messageId = messageId;
    this.contextId = contextId;
    this.key = key;
    this.topic = topic;
    this.partition = partition;
    this.headers = {
      __LW_messageId: this.messageId,
      __LW_contextId: this.contextId,
      ...headers,
    };
  }

  getContextFrom(anotherMessage) {
    this.contextId = anotherMessage.contextId;
  }

  static parse(rawMessage) {
    return {
      ...rawMessage,
      messageId: rawMessage.headers.__LW_messageId,
      contextId: rawMessage.headers.__LW_contextId,
    };
  }
}

class MuxedMessage extends Message {

  static isMuxed({ headers }) {
    return headers.__LW_messageType === 'muxed';
  }

  static demux(message) {
    const {
      __LW_messageType: messageType,
      __LW_mux: mux,
    } = message.headers;

    if (messageType !== 'muxed') {
      throw new DemuxError(
        'Message is not muxed',
        'MESSAGE_NOT_MUXED_ERR',
        { message }
      );
    }

    return JSON.parse(mux).map((
      {
        headStart,
        headEnd,
        valueStart,
        valueEnd,
      }) => {
      const meta = message.value.slice(headStart, headEnd);
      const value = message.value.slice(valueStart, valueEnd);

      return super.parse({
        value,
        ...JSON.parse(meta.toString('utf-8')),
      });
    });
  }

  constructor(...args) {
    super(...args);

    this.headers = {
      ...this.headers,
      __LW_messageType: 'muxed'
    };

    this.value = Buffer.alloc(0);

    Object.defineProperty(this, '_mux', {
      value: [],
      enumerable: false,
      configurable: true,
    });
  }

  mux(message) {
    // A message head is an object containing every property
    // of a message, except for the `value` property.
    const messageHead = Object.assign({}, message);
    delete messageHead.value;

    const bufferedHead = Buffer.from(JSON.stringify(messageHead));
    const messageValue = Buffer.from(message.value);

    const headStart = this.value.length;
    const headEnd = headStart + bufferedHead.length;
    const valueStart = headEnd;
    const valueEnd = headEnd + messageValue.length;

    this._mux.push({ headStart, headEnd, valueStart, valueEnd });

    this.value = Buffer.concat([this.value, bufferedHead, messageValue]);
    this.headers.__LW_mux = JSON.stringify(this._mux);

    return this;
  }
}

module.exports = {
  MuxedMessage,
  Message,
};

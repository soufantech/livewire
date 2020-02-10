const uuid = require('uuid/v4');
const { DemuxError } = require('./error');

class BaseMessage {

  constructor({
    value,
    key,
    partition,
    topic,
    headers = {},
    messageId = uuid(),
  }) {
    this.value = value;
    this.messageId = messageId;
    this.key = key;
    this.topic = topic;
    this.partition = partition;
    this.headers = {
      LW_messageId: this.messageId,
      ...headers,
    };
  }
}

class Message extends BaseMessage {
  constructor(args) {
    super(args);

    this.headers = {
      LW_messageType: 'generic',
      ...this.headers,
    };
  }

  static parse(message) {
    return { ...message, messageId: message.headers.LW_messageId };
  }
}

class MuxedMessage extends BaseMessage {

  static isMuxed({ headers }) {
    return headers.LW_messageType === 'muxed';
  }

  static demux(message) {
    const {
      LW_messageType: messageType,
      LW_mux: mux,
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
        valueEnd
      }) => {
      const meta = message.value.slice(headStart, headEnd);
      const value = message.value.slice(valueStart, valueEnd);

      return {
        value,
        ...JSON.parse(meta.toString('utf-8')),
      };
    });
  }

  constructor(...args) {
    super(...args);

    this.headers = { LW_messageType: 'muxed' };
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
    this.headers.LW_mux = JSON.stringify(this._mux);

    return this;
  }
}

module.exports = {
  MuxedMessage,
  Message,
};

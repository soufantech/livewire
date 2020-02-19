const uuid = require('uuid/v4');
const { DemuxError } = require('./error');

const LIVEWIRE_KEY_PREFIX = '__LW_';
const MESSAGE_ID_KEY = LIVEWIRE_KEY_PREFIX + 'messageId';
const MESSAGE_TYPE_KEY = LIVEWIRE_KEY_PREFIX + 'messageType';
const MUX_KEY = LIVEWIRE_KEY_PREFIX + 'mux';

const filterHeaders = headers => {
  return Object
    .entries(headers)
    .reduce((acc, [k, v]) => {
      if (k.startsWith(LIVEWIRE_KEY_PREFIX)) return acc;
      acc[k] = v;
      return acc;
    }, {});
};

const filterLivewireMetadata = headers => {
  return Object
    .entries(headers)
    .reduce((acc, [k, v]) => {
      if (!k.startsWith(LIVEWIRE_KEY_PREFIX)) return acc;
      acc[k.replace(LIVEWIRE_KEY_PREFIX, '')] = v;
      return acc;
    }, {});
};

class Message {
  constructor({
    value,
    key,
    partition,
    topic,
    headers = {},
    messageId = uuid(),
    messageType = 'generic',
  }) {
    Object.defineProperty(this, 'messageType', { value: messageType });

    this.value = value;
    this.messageId = messageId;
    this.key = key;
    this.topic = topic;
    this.partition = partition;
    this.headers = {
      [MESSAGE_ID_KEY]: this.messageId,
      [MESSAGE_TYPE_KEY]: this.messageType,
      ...headers,
    };
  }

  toString() {
    return `[${this.constructor.name} ${this.messageId}]`;
  }

  static parse({
    value,
    headers = {},
    key,
    offset,
    partition,
    topic,
    timestamp
  }) {
    const filteredHeaders = filterHeaders(headers);
    const livewireMetadata = filterLivewireMetadata(headers);

    const metadata = {
      ...livewireMetadata,
      key,
      offset,
      topic,
      partition,
      timestamp,
    };

    return {
      value,
      metadata,
      headers: filteredHeaders,
    };
  }
}

class MuxedMessage extends Message {

  static isMuxed({ metadata }) {
    return metadata.messageType === 'muxed';
  }

  static demux(message) {
    const { metadata } = message;

    if (metadata.messageType !== 'muxed') {
      throw new DemuxError(
        'Message is not muxed',
        'MESSAGE_NOT_MUXED_ERR',
        { message }
      );
    }

    return JSON.parse(metadata.mux).map((
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

  constructor(args) {
    super({ ...args, messageType: 'muxed' });

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
    this.headers[MUX_KEY] = JSON.stringify(this._mux);

    return this;
  }
}

module.exports = {
  MuxedMessage,
  Message,
};

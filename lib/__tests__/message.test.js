const { Message, MuxedMessage } = require('../message')
const { DemuxError } = require('../error')

const simulateMessageFromBroker = msg => {
  return {
    key: msg.key == null ? msg.key : Buffer.from(msg.key),
    partition: msg.partition,
    topic: msg.topic,
    value: msg.value,
    headers: msg.headers,
    timestamp: (new Date(2019, 1, 20)).getTime(),
  };
};

describe('message', () => {

  describe('Message', () => {

    it('exposes an outboxable message interface.', () => {
      const msg = new Message({
        key: '135235',
        value: Buffer.from('Number 1!'),
        topic: 'numbers',
        partition: 1,
        headers: {
          cardinal: 'one',
          ordinal: 'first',
        }
      });

      expect(msg).toEqual(expect.objectContaining({
        contextId: expect.any(String),
        messageId: expect.any(String),
        key: '135235',
        value: expect.any(Buffer),
        topic: 'numbers',
        partition: 1,
        headers: expect.objectContaining({
          cardinal: 'one',
          ordinal: 'first',
        }),
      }));
    });

    it('can receive the contextId from another message.', () => {
      const msg1 = new Message({
        key: '135235',
        value: Buffer.from('Number 1!'),
        topic: 'numbers',
        partition: 1,
        headers: {
          cardinal: 'one',
          ordinal: 'first',
        }
      });

      const msg2 = new Message({
        key: '135235',
        value: Buffer.from('Number 1!'),
        topic: 'numbers',
        partition: 1,
        headers: {
          cardinal: 'one',
          ordinal: 'first',
        }
      });

      const msg2ContextId = msg2.contextId;

      // Sanity check:
      expect(msg1.contextId).not.toBe(msg2ContextId);

      msg1.getContextFrom(msg2);

      expect(msg1.contextId).toBe(msg2ContextId);
      expect(msg2.contextId).toBe(msg2ContextId);
    });

    it('parses a message from the broker.', () => {
      const msg = new Message({
        key: '135235',
        value: Buffer.from('Number 1!'),
        topic: 'numbers',
        partition: 1,
        headers: {
          cardinal: 'one',
          ordinal: 'first',
        }
      });

      const incomingMessage = simulateMessageFromBroker(msg);

      expect(Message.parse(incomingMessage)).toEqual(expect.objectContaining({
        contextId: expect.any(String),
        messageId: expect.any(String),
        key: expect.any(Buffer),
        value: expect.any(Buffer),
        topic: 'numbers',
        timestamp: expect.any(Number),
        partition: 1,
        headers: expect.objectContaining({
          cardinal: 'one',
          ordinal: 'first',
        }),
      }));
    });
  });

  describe('MuxedMessage', () => {

    it('exposes an outboxable message interface.', () => {
      const muxed = new MuxedMessage({
        key: '135235',
        topic: 'numbers',
        partition: 1,
        headers: {
          cardinal: 'one',
          ordinal: 'first',
        }
      });

      expect(muxed).toEqual(expect.objectContaining({
        contextId: expect.any(String),
        messageId: expect.any(String),
        key: '135235',
        value: expect.any(Buffer),
        topic: 'numbers',
        partition: 1,
        headers: expect.objectContaining({
          cardinal: 'one',
          ordinal: 'first',
        }),
      }));
    });

    it('muxes and demuxes messages.', () => {
      const msg1 = new Message({
        key: '135235',
        value: Buffer.from('Number 1!'),
        topic: 'numbers',
        partition: 1,
        headers: {
          cardinal: 'one',
          ordinal: 'first',
        }
      });

      const msg2 = new Message({
        key: '2920502',
        value: Buffer.from('Number 2!'),
        topic: 'numbers',
        partition: 2,
        headers: {
          cardinal: 'two',
          ordinal: 'second',
        }
      });

      const msg3 = new Message({
        key: '3523523',
        value: Buffer.from('Number 3!'),
        topic: 'numbers',
        partition: 3,
        headers: {
          cardinal: 'three',
          ordinal: 'third',
        }
      });

      const muxed = new MuxedMessage({
        key: '3523523',
        topic: 'numbers',
        partition: 2,
      });

      muxed
        .mux(msg1)
        .mux(msg2)
        .mux(msg3);

      const incomingMessage = simulateMessageFromBroker(muxed);
      const demuxed = MuxedMessage.demux(incomingMessage);

      expect(demuxed.length).toBe(3);
      
      expect(demuxed[0]).toEqual(expect.objectContaining({
        contextId: msg1.contextId,
        messageId: msg1.messageId,
        key: msg1.key,
        value: msg1.value,
        topic: msg1.topic,
        partition: msg1.partition,
        headers: msg1.headers,
      }));

      expect(demuxed[1]).toEqual(expect.objectContaining({
        contextId: msg2.contextId,
        messageId: msg2.messageId,
        key: msg2.key,
        value: msg2.value,
        topic: msg2.topic,
        partition: msg2.partition,
        headers: msg2.headers,
      }));

      expect(demuxed[2]).toEqual(expect.objectContaining({
        contextId: msg3.contextId,
        messageId: msg3.messageId,
        key: msg3.key,
        value: msg3.value,
        topic: msg3.topic,
        partition: msg3.partition,
        headers: msg3.headers,
      }));
    });

    it('checks if a broker message is a muxed message', () => {
      const commonMessage = new Message({
        key: '135235',
        value: Buffer.from('Number 1!'),
        topic: 'numbers',
        partition: 1,
        headers: {
          cardinal: 'one',
          ordinal: 'first',
        }
      });

      const muxedMessage = new MuxedMessage({
        key: '3523523',
        topic: 'numbers',
        partition: 2,
      });

      const incomingCommonMessage = simulateMessageFromBroker(commonMessage);
      const incomingMuxedMessage = simulateMessageFromBroker(muxedMessage);
      
      expect(MuxedMessage.isMuxed(incomingCommonMessage)).toBe(false);
      expect(MuxedMessage.isMuxed(incomingMuxedMessage)).toBe(true);
    });

    it('throws an error when demuxing a non muxed message.', () => {
      const nonMuxedMessage = new Message({
        value: Buffer.from('I am a non muxed message!'),
        topic: 'nonmuxed-messages',
        key: '3523523',
        headers: {
          hello: 'hello',
        }
      });

      const incomingMessage = simulateMessageFromBroker(nonMuxedMessage);

      expect(() => MuxedMessage.demux(incomingMessage)).toThrow(DemuxError);
    });

    it('exposes a message interface.', () => {
      const msg = new Message({
        key: '135235',
        value: Buffer.from('Number 1!'),
        topic: 'numbers',
        partition: 1,
        headers: {
          cardinal: 'one',
          ordinal: 'first',
        }
      });

      const muxed = new MuxedMessage({
        key: '3523523',
        topic: 'numbers',
        partition: 2,
      });

      muxed.mux(msg);

      expect(muxed).toEqual(expect.objectContaining({
        contextId: expect.any(String),
        messageId: expect.any(String),
        key: '3523523',
        value: expect.any(Buffer),
        topic: 'numbers',
        partition: 2,
        headers: expect.any(Object),
      }));
    });
  });
});

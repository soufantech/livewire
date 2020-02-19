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
    offset: 235,
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

    it('has a string representation.', () => {
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

      const messageId = msg.messageId;
      const sgringifiedMessage = msg.toString();

      expect(sgringifiedMessage).toEqual(expect.stringContaining('Message'));
      expect(sgringifiedMessage).toEqual(expect.stringContaining(messageId));
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

      expect(Message.parse(incomingMessage)).toEqual({
        metadata: expect.objectContaining({
          messageId: expect.any(String),
          key: expect.any(Buffer),
          topic: 'numbers',
          timestamp: expect.any(Number),
          partition: 1,
          offset: expect.any(Number),
        }),
        headers: expect.objectContaining({
          cardinal: 'one',
          ordinal: 'first',
        }),
        value: expect.any(Buffer),
      });
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

    it('has a string representation.', () => {
      const msg = new MuxedMessage({
        key: '135235',
        value: Buffer.from('Number 1!'),
        topic: 'numbers',
        partition: 1,
        headers: {
          cardinal: 'one',
          ordinal: 'first',
        }
      });

      const messageId = msg.messageId;
      const sgringifiedMessage = msg.toString();

      expect(sgringifiedMessage).toEqual(expect.stringContaining('MuxedMessage'));
      expect(sgringifiedMessage).toEqual(expect.stringContaining(messageId));
    });

    it('is parseable', () => {
      const msg = new MuxedMessage({
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

      expect(Message.parse(incomingMessage)).toEqual({
        metadata: expect.objectContaining({
          messageId: expect.any(String),
          key: expect.any(Buffer),
          topic: 'numbers',
          timestamp: expect.any(Number),
          partition: 1,
          offset: expect.any(Number),
        }),
        headers: expect.objectContaining({
          cardinal: 'one',
          ordinal: 'first',
        }),
        value: expect.any(Buffer),
      });
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
      const parsedMessage = Message.parse(incomingMessage);
      const demuxedMessages = MuxedMessage.demux(parsedMessage);

      expect(demuxedMessages.length).toBe(3);
      
      expect(demuxedMessages[0]).toEqual({
        metadata: expect.objectContaining({
          messageId: msg1.messageId,
          key: msg1.key,
          topic: msg1.topic,
          partition: msg1.partition,
        }),
        headers: expect.objectContaining({
          cardinal: 'one',
          ordinal: 'first',
        }),
        value: expect.any(Buffer),
      });

      expect(demuxedMessages[1]).toEqual({
        metadata: expect.objectContaining({
          messageId: msg2.messageId,
          key: msg2.key,
          topic: msg2.topic,
          partition: msg2.partition,
        }),
        headers: expect.objectContaining({
          cardinal: 'two',
          ordinal: 'second',
        }),
        value: expect.any(Buffer),
      });

      expect(demuxedMessages[2]).toEqual({
        metadata: expect.objectContaining({
          messageId: msg3.messageId,
          key: msg3.key,
          topic: msg3.topic,
          partition: msg3.partition,
        }),
        headers: expect.objectContaining({
          cardinal: 'three',
          ordinal: 'third',
        }),
        value: expect.any(Buffer),
      });
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

      const parsedCommonMessage = Message.parse(incomingCommonMessage);
      const parsedMuxedMessage = Message.parse(incomingMuxedMessage);
      
      expect(MuxedMessage.isMuxed(parsedCommonMessage)).toBe(false);
      expect(MuxedMessage.isMuxed(parsedMuxedMessage)).toBe(true);
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
      const parsedMessage = Message.parse(incomingMessage);

      expect(() => MuxedMessage.demux(parsedMessage)).toThrow(DemuxError);
    });
  });
});

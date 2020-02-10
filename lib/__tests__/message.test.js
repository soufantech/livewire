const { Message, MuxedMessage } = require('../message')
const { DemuxError } = require('../error')

describe('message', () => {

  describe('Message', () => {

    it('exposes a message interface.', () => {
      const msg = new Message({
        key: 135235,
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
        key: 135235,
        value: expect.any(Buffer),
        topic: 'numbers',
        partition: 1,
        headers: expect.objectContaining({
          cardinal: 'one',
          ordinal: 'first',
        }),
      }));
    });
  });

  describe('MuxedMessage', () => {

    it('muxes and demuxes messages.', () => {

      const msg1 = new Message({
        key: 135235,
        value: Buffer.from('Number 1!'),
        topic: 'numbers',
        partition: 1,
        headers: {
          cardinal: 'one',
          ordinal: 'first',
        }
      });

      const msg2 = new Message({
        key: 2920502,
        value: Buffer.from('Number 2!'),
        topic: 'numbers',
        partition: 2,
        headers: {
          cardinal: 'two',
          ordinal: 'second',
        }
      });

      const msg3 = new Message({
        key: 3523523,
        value: Buffer.from('Number 3!'),
        topic: 'numbers',
        partition: 3,
        headers: {
          cardinal: 'three',
          ordinal: 'third',
        }
      });

      const muxed = new MuxedMessage({
        key: 3523523,
        topic: 'numbers',
        partition: 2,
      });

      muxed
        .mux(msg1)
        .mux(msg2)
        .mux(msg3);

      const demuxed = MuxedMessage.demux(muxed);

      expect(demuxed.length).toBe(3);
      expect(demuxed[0]).toEqual(msg1);
      expect(demuxed[1]).toEqual(msg2);
      expect(demuxed[2]).toEqual(msg3);
    });

    it('throws an error when demuxing a non muxed message.', () => {
      const nonMuxedMessage = new Message({
        value: Buffer.from('I am a non muxed message!'),
        topic: 'nonmuxed-messages',
        headers: {
          I: 'I',
          am: 'am',
          not: 'not',
          a: 'a',
          muxed: 'muxed',
          message: 'message',
        }
      });

      expect(() => MuxedMessage.demux(nonMuxedMessage)).toThrow(DemuxError);
    });

    it('exposes a message interface.', () => {
      const msg = new Message({
        key: 135235,
        value: Buffer.from('Number 1!'),
        topic: 'numbers',
        partition: 1,
        headers: {
          cardinal: 'one',
          ordinal: 'first',
        }
      });

      const muxed = new MuxedMessage({
        key: 3523523,
        topic: 'numbers',
        partition: 2,
      });

      muxed.mux(msg);

      expect(muxed).toEqual(expect.objectContaining({
        messageId: expect.any(String),
        key: 3523523,
        value: expect.any(Buffer),
        topic: 'numbers',
        partition: 2,
        headers: expect.any(Object),
      }));
    });
  })
});

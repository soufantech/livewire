module.exports = ({ storageAdapter }) => {

  const post = async (messages, storageAdapterOptions) => {
    // const messages = [].concat(outboxable).map(o => o.toOutbox());
    messages = [].concat(messages);

    await storageAdapter.save(messages, storageAdapterOptions);
  }

  const getUncleared = () => {
    return storageAdapter.getUncleared();
  };

  const watch = (onError, onData) => {
    return storageAdapter.watch(onError, onData);
  };

  const clear = async message => {
    const { messageId } = message;

    await storageAdapter.clear(messageId);
  };

  const relay = async (onError, onMessage) => {
    const send = async message => {
      await onMessage(message);
      await clear(message);
    };

    await watch(onError, async message => {
      await send(message).catch(onError);
    });

    const uncleared = await getUncleared();
    const promisses = uncleared.map(message => send(message));

    await Promise.all(promisses).catch(onError);
  }

  return {
    post,
    clear,
    relay,
    watch,
    getUncleared,
  };
};

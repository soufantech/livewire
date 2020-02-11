/**
 * @deprecated use outbox.relay instead.
 */
module.exports = ({ onError, onMessage, outbox }) => {

  const catchup = async () => {
    const uncleared = await outbox.getUncleared();
    const promisses = uncleared.map(message => send(message));

    return Promise.all(promisses).catch(onError);
  };

  const send = async message => {
    await onMessage(message);

    return outbox.clear(message);
  };

  const watch = () => {
    return outbox.watch(onError, message => {
      return send(message).catch(onError);
    });
  };

  const run = () => {
    watch();

    return catchup();
  };

  return {
    run,
    watch,
    catchup,
  };
};

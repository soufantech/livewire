module.exports = ({ storageAdapter }) => {

  const log = async (message, storageAdapterOptions) => {
    await storageAdapter.save(message, storageAdapterOptions);
  };

  return {
    log,
  };
};

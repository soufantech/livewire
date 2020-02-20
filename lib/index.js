const error = require('./error');
const message = require('./message');
const dispatcher = require('./dispatcher');
const createOutbox = require('./outbox');
const createInbox = require('./inbox');

class Livewire {
  inbox({ storageAdapter } = {}) {
    if (this._inbox) return this._inbox;

    this._inbox = createInbox({ storageAdapter });

    return this._inbox;
  }

  outbox({ storageAdapter } = {}) {
    if (this._outbox) return this._outbox;

    this._outbox = createOutbox({ storageAdapter });

    return this._outbox;
  }
}

module.exports = {
  Livewire,
  ...error,
  ...message,
  ...dispatcher,
};

const { DispatcherMatchError } = require('./error');

const matchers = Object.freeze({
  objectContaining: (spec, subject) =>
    Object.keys(spec).every(k => spec[k] === subject[k]),
});

class Dispatcher {
  static get matchers() {
    return matchers;
  }

  constructor({ matcher } = {}) {
    this._matcher = matchers.objectContaining;

    if (typeof matcher === 'function') {
      this._matcher = matcher;
    }

    this._actions = [];
  }

  register(spec, action) {
    this._actions.push([spec, action]);

    return this;
  }

  dispatch(subject) {
    const match = this._actions.find(([spec]) => this._matcher(spec, subject));

    if (match) {
      const [, action] = match;
      return action;
    }

    throw new DispatcherMatchError(`No match`, 'NO_MATCH', { subject });
  }
}

module.exports = {
  Dispatcher,
};

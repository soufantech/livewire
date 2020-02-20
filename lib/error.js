/* eslint-disable max-classes-per-file */
class LivewireError extends Error {
  constructor(err, code, extensions) {
    super(err);

    this.message = err || err.message;
    this.code = code;
    this.extensions = extensions;

    Object.defineProperty(this, 'name', { value: this.constructor.name });

    Error.captureStackTrace(this, this.constructor);
  }
}

class DemuxError extends LivewireError {
  constructor(err, code, { message }) {
    super(err, code, { message });
  }
}

class DispatcherMatchError extends LivewireError {
  constructor(e, code, { subject }) {
    super(e, code, { subject });
  }
}

class DuplicatedInboxMessageError extends LivewireError {
  constructor({ message, originalError }) {
    super('Duplicated inbox message', 'DUPLICATED_INBOX_MESSAGE_ERROR', {
      message,
      originalError,
    });
  }
}

class DuplicatedOutboxMessageError extends LivewireError {
  constructor({ messages, originalError }) {
    super('Duplicated outbox message', 'DUPLICATED_OUTBOX_MESSAGE_ERROR', {
      messages,
      originalError,
    });
  }
}

class NoOutboxMessageError extends LivewireError {
  constructor({ messageId }) {
    super(
      `No outbox message with messageId ${messageId}`,
      'NO_OUTBOX_MESSAGE_ERROR',
      { messageId }
    );
  }
}

module.exports = {
  DemuxError,
  LivewireError,
  DispatcherMatchError,
  NoOutboxMessageError,
  DuplicatedOutboxMessageError,
  DuplicatedInboxMessageError,
};

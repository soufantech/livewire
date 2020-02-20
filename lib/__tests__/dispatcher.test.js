const { Dispatcher } = require('../dispatcher');
const { DispatcherMatchError } = require('../error');

describe('Dispatcher', () => {
  it('dispatches an action when there is a match.', () => {
    const dispatcher = new Dispatcher();

    const action = jest.fn();

    dispatcher.register({ a: 'A', b: 6, c: true }, action);

    const dispatchedAction = dispatcher.dispatch({
      a: 'A',
      b: 6,
      c: true,
      d: Symbol('anything'),
    });

    expect(action).toBe(dispatchedAction);
  });

  it('dispatches the first registered action when there are multiple matches.', () => {
    const dispatcher = new Dispatcher();

    const firstAction = jest.fn();
    const secondAction = jest.fn();
    const thirdAction = jest.fn();

    const generalSpec = { a: 1, b: 2, c: 3 };

    dispatcher
      .register(generalSpec, firstAction)
      .register(generalSpec, secondAction)
      .register(generalSpec, thirdAction);

    const dispatchedAction = dispatcher.dispatch(generalSpec);

    expect(firstAction).toBe(dispatchedAction);
  });

  it('throws an error if no match is found.', () => {
    const dispatcher = new Dispatcher();

    const action = jest.fn();

    dispatcher.register(
      { a: 'A', b: 6, c: true, d: 'UNMATCHED ATTRIBUTE' },
      action
    );

    expect(() => {
      dispatcher.dispatch({
        a: 'A',
        b: 6,
        c: true,
        d: 'X',
      });
    }).toThrow(DispatcherMatchError);
  });

  it('dispatches using a custom matcher.', () => {
    const dispatcher = new Dispatcher({
      matcher: (a, b) => a === b,
    });

    const action1 = jest.fn();
    const action2 = jest.fn();
    const action3 = jest.fn();

    dispatcher.register(1, action1);
    dispatcher.register(2, action2);
    dispatcher.register(3, action3);

    const dispatchedAction = dispatcher.dispatch(2);

    expect(action2).toBe(dispatchedAction);
  });
});

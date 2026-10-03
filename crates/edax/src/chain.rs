//! The handler registry behind [`EventRouter`](crate::EventRouter): a map of handlers by name plus the
//! middleware chain baked around each one at register time. [`CommandBus`](crate::CommandBus) bakes
//! its chains per command type with the same fold.

use std::collections::HashMap;

use edax_core::ErrorCode;

use crate::Error;

/// Wrap `handler` in `middleware`, the first one outermost: fold inner-to-outer, in reverse.
pub(crate) fn wrap_outermost_first<H, M: Fn(H) -> H>(handler: H, middleware: &[M]) -> H {
    middleware.iter().rev().fold(handler, |handler, mw| mw(handler))
}

/// Handlers by name, with a middleware chain applied when a handler is registered. `M` is the
/// middleware type of the path, so each path keeps its own `Send + Sync` bounds.
pub(crate) struct Chain<H, M> {
    handlers: HashMap<String, H>,
    middleware: Vec<M>,
}

impl<H, M: Fn(H) -> H> Chain<H, M> {
    pub(crate) fn new() -> Self {
        Self { handlers: HashMap::new(), middleware: Vec::new() }
    }

    /// Append a middleware. The first one added ends up outermost.
    pub(crate) fn push_middleware(&mut self, mw: M) {
        self.middleware.push(mw);
    }

    /// Install `handler` under `name`, wrapped in the chain as it stands now. The terminal
    /// [`ErrorCode::AlreadyRegistered`] on a duplicate name.
    pub(crate) fn register(&mut self, name: String, handler: H) -> Result<(), Error> {
        if self.handlers.contains_key(&name) {
            return Err(Error::new(ErrorCode::AlreadyRegistered, name));
        }
        let wrapped = self.wrap(handler);
        self.handlers.insert(name, wrapped);
        Ok(())
    }

    /// Wrap `handler` in the chain without registering it, for a fallback handler.
    pub(crate) fn wrap(&self, handler: H) -> H {
        wrap_outermost_first(handler, &self.middleware)
    }

    pub(crate) fn get(&self, name: &str) -> Option<&H> {
        self.handlers.get(name)
    }
}

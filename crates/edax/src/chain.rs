//! The handler registry shared by [`CommandBus`](crate::CommandBus) and
//! [`EventRouter`](crate::EventRouter): a map of handlers by name plus the middleware chain baked
//! around each one at register time.

use std::collections::HashMap;

use crate::Error;

/// Handlers by name, with a middleware chain applied when a handler is registered. `M` is the
/// middleware type of the path (command or event), so each path keeps its own `Send + Sync` bounds.
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

    /// Install `handler` under `name`, wrapped in the chain as it stands now.
    /// [`Error::AlreadyRegistered`] on a duplicate name.
    pub(crate) fn register(&mut self, name: String, handler: H) -> Result<(), Error> {
        if self.handlers.contains_key(&name) {
            return Err(Error::AlreadyRegistered(name));
        }
        let wrapped = self.wrap(handler);
        self.handlers.insert(name, wrapped);
        Ok(())
    }

    /// Wrap `handler` in the chain without registering it, for a fallback handler.
    pub(crate) fn wrap(&self, handler: H) -> H {
        // First middleware ends up outermost: wrap inner-to-outer by folding in reverse.
        self.middleware.iter().rev().fold(handler, |handler, mw| mw(handler))
    }

    pub(crate) fn get(&self, name: &str) -> Option<&H> {
        self.handlers.get(name)
    }
}

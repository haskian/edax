//! The handler registry behind [`EventRouter`](crate::EventRouter): a map of handlers by name plus the
//! middleware chain baked around each one at register time. [`CommandBus`](crate::CommandBus) bakes
//! its chains per command type with the same fold, and refuses a second handler by the same rule.

use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::hash::Hash;

use edax_core::ErrorCode;

use crate::Error;

/// Wrap `handler` in `middleware`, the first one outermost: fold inner-to-outer, in reverse.
pub(crate) fn wrap_outermost_first<H, M: Fn(H) -> H>(handler: H, middleware: &[M]) -> H {
    middleware.iter().rev().fold(handler, |handler, mw| mw(handler))
}

/// Install the handler `build` makes under `name`, unless `name` already has one: then the terminal
/// [`ErrorCode::AlreadyRegistered`] naming it, and `build` is never called. The one duplicate rule of every
/// registry, so each refuses alike.
pub(crate) fn register_once<K: Eq + Hash + AsRef<str>, V>(
    handlers: &mut HashMap<K, V>,
    name: K,
    build: impl FnOnce() -> V,
) -> Result<(), Error> {
    match handlers.entry(name) {
        Entry::Occupied(taken) => Err(Error::new(ErrorCode::AlreadyRegistered, taken.key().as_ref())),
        Entry::Vacant(free) => {
            free.insert(build());
            Ok(())
        }
    }
}

/// Handlers by name, with a middleware chain applied when a handler is registered. `M` is the
/// middleware type of the path, so each path keeps its own `Send + Sync` bounds.
pub(crate) struct Chain<H, M> {
    handlers: HashMap<String, H>,
    middleware: Vec<M>,
}

impl<H, M: Fn(H) -> H> Chain<H, M> {
    /// A registry whose every handler is wrapped in `middleware`, the first one outermost. The chain
    /// is fixed here, so no handler can be registered before a middleware that should wrap it.
    pub(crate) fn new(middleware: Vec<M>) -> Self {
        Self {
            handlers: HashMap::new(),
            middleware,
        }
    }

    /// Install `handler` under `name`, wrapped in the chain. The terminal
    /// [`ErrorCode::AlreadyRegistered`] on a duplicate name.
    pub(crate) fn register(&mut self, name: String, handler: H) -> Result<(), Error> {
        let middleware = &self.middleware;
        register_once(&mut self.handlers, name, || wrap_outermost_first(handler, middleware))
    }

    /// Wrap `handler` in the chain without registering it, for a fallback handler.
    pub(crate) fn wrap(&self, handler: H) -> H {
        wrap_outermost_first(handler, &self.middleware)
    }

    pub(crate) fn get(&self, name: &str) -> Option<&H> {
        self.handlers.get(name)
    }
}

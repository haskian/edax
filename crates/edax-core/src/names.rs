//! The one shape every closed set of names a kernel decision crosses as, and the one rule an open name a
//! host chooses follows: an extension key, a rejection's reason.

/// A unit enum whose variants cross a binding by name: the enum, its `ALL` and `NAMES` lists, its
/// `as_str` and its `by_name`, from one row list.
///
/// A binding checks its own list of names against `ALL` at import, so a name the kernel adds fails the
/// import instead of landing in a neighbouring branch. That check is only as good as `ALL`: written by
/// hand, a variant left out of it still compiles. One row list makes the omission impossible, and a name
/// spelled twice fails the build.
///
/// ```
/// edax_core::closed_names! {
///     /// A light.
///     #[derive(Debug, Clone, Copy, PartialEq, Eq)]
///     pub enum Light {
///         /// Stop.
///         Red = "red",
///         /// Go.
///         Green = "green",
///     }
/// }
/// assert_eq!(Light::ALL, &[Light::Red, Light::Green]);
/// assert_eq!(Light::NAMES, &["red", "green"]);
/// assert_eq!(Light::Green.as_str(), "green");
/// assert_eq!(Light::by_name("green"), Some(Light::Green));
/// ```
#[macro_export]
macro_rules! closed_names {
    (
        $(#[$meta:meta])*
        $vis:vis enum $name:ident {
            $( $(#[$variant_meta:meta])* $variant:ident = $text:literal ),+ $(,)?
        }
    ) => {
        $(#[$meta])*
        $vis enum $name {
            $( $(#[$variant_meta])* $variant, )+
        }

        const _: () = assert!($crate::distinct_names(&[ $( $text, )+ ]), "two variants share a name");

        impl $name {
            /// Every variant, in declaration order. A binding checks its own list of names against this one.
            pub const ALL: &'static [$name] = &[ $( $name::$variant, )+ ];

            /// Every variant's name, in declaration order: what a binding hands its host to check against.
            pub const NAMES: &'static [&'static str] = &[ $( $text, )+ ];

            /// The stable name every binding reads, one per variant.
            pub fn as_str(self) -> &'static str {
                match self {
                    $( $name::$variant => $text, )+
                }
            }

            /// The variant named `name`, the inverse of `as_str`. `None` for a name no variant has; the
            /// caller words its own refusal.
            pub fn by_name(name: &str) -> Option<$name> {
                match name {
                    $( $text => Some($name::$variant), )+
                    _ => None,
                }
            }
        }
    };
}

/// The longest name [`is_open_name`] accepts, in bytes.
pub const OPEN_NAME_MAX: usize = 64;

/// Whether `name`, one a host chooses, follows the rule every such name follows: 1 to
/// [`OPEN_NAME_MAX`] bytes of lowercase ASCII letters, digits, `.`, `_` and `-`, starting with a letter
/// or a digit. A dot namespaces a name (`http.origin_ip`). Lowercase keeps it a safe header name on every
/// transport, and the first character keeps it from reading as a flag or a path.
pub fn is_open_name(name: &str) -> bool {
    let bytes = name.as_bytes();
    let first_ok = bytes
        .first()
        .is_some_and(|b| b.is_ascii_lowercase() || b.is_ascii_digit());
    let rest_ok = bytes
        .iter()
        .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || matches!(b, b'.' | b'_' | b'-'));
    first_ok && rest_ok && bytes.len() <= OPEN_NAME_MAX
}

/// Whether no two of `names` are equal. A `const fn`, so [`closed_names!`] checks its rows at compile
/// time.
#[doc(hidden)]
pub const fn distinct_names(mut names: &[&str]) -> bool {
    while let [first, rest @ ..] = names {
        let mut others = rest;
        while let [other, tail @ ..] = others {
            if same_bytes(first.as_bytes(), other.as_bytes()) {
                return false;
            }
            others = tail;
        }
        names = rest;
    }
    true
}

const fn same_bytes(mut a: &[u8], mut b: &[u8]) -> bool {
    loop {
        match (a, b) {
            ([], []) => return true,
            ([x, a_rest @ ..], [y, b_rest @ ..]) if *x == *y => {
                a = a_rest;
                b = b_rest;
            }
            _ => return false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::distinct_names;

    #[test]
    fn distinct_names_branches() {
        for (name, names, want) in [
            ("none", &[][..], true),
            ("one", &["a"][..], true),
            ("distinct, a prefix included", &["a", "ab", "b"][..], true),
            ("a repeat", &["a", "b", "a"][..], false),
        ] {
            assert_eq!(distinct_names(names), want, "{name}");
        }
    }
}

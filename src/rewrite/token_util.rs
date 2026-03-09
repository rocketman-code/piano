use proc_macro2::{Spacing, TokenTree};

/// Check if tokens[i] and tokens[i+1] form `=>`.
pub(super) fn is_fat_arrow(tts: &[TokenTree], i: usize) -> bool {
    if i + 1 >= tts.len() {
        return false;
    }
    matches!(
        (&tts[i], &tts[i + 1]),
        (
            TokenTree::Punct(a),
            TokenTree::Punct(b),
        ) if a.as_char() == '=' && a.spacing() == Spacing::Joint && b.as_char() == '>'
    )
}

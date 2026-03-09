use proc_macro2::{Delimiter, Group, Ident, Punct, Spacing, Span, TokenStream, TokenTree};

/// Quick string-level check for whether source contains a crossbeam select
/// macro. Used to skip the token round-trip when unnecessary, preserving
/// whitespace in the common case.
pub fn source_has_crossbeam_select(source: &str) -> bool {
    source.contains("crossbeam_channel")
        && (source.contains("select!") || source.contains("select_biased!"))
}

/// Inject `channel.stats.record_recv_if_ok(&result)` or
/// `channel.stats.record_send_if_ok(&result)` at the start of each recv/send
/// arm body inside crossbeam `select!` / `select_biased!` invocations.
///
/// This is token-level manipulation of the user's macro invocation, not the
/// macro expansion. It works in tandem with the Deref proxy: select! bypasses
/// ProxyReceiver's Deref coercion by calling Receiver::recv directly, so we
/// inject the stats recording call into each arm body to close the gap.
pub fn inject_select_arm_stats(tokens: TokenStream) -> TokenStream {
    let tts: Vec<TokenTree> = tokens.into_iter().collect();
    let result = process_tokens(&tts);
    result.into_iter().collect()
}

/// Walk tokens, looking for crossbeam select! invocations. When found, process
/// the macro body. Otherwise, recurse into any groups to handle nested cases.
fn process_tokens(tts: &[TokenTree]) -> Vec<TokenTree> {
    let mut out: Vec<TokenTree> = Vec::with_capacity(tts.len());
    let mut i = 0;

    while i < tts.len() {
        let (path_len, is_select) = match_crossbeam_select_path(tts, i);
        if is_select {
            // Copy the path tokens (crossbeam_channel :: select !) verbatim.
            for tt in &tts[i..i + path_len] {
                out.push(tt.clone());
            }
            i += path_len;

            // The next token should be the macro body group `{ ... }`.
            if i < tts.len() {
                if let TokenTree::Group(group) = &tts[i] {
                    if group.delimiter() == Delimiter::Brace {
                        let inner: Vec<TokenTree> = group.stream().into_iter().collect();
                        let injected = inject_into_select_body(&inner);
                        let new_stream: TokenStream = injected.into_iter().collect();
                        let mut new_group = Group::new(Delimiter::Brace, new_stream);
                        new_group.set_span(group.span());
                        out.push(TokenTree::Group(new_group));
                        i += 1;
                        continue;
                    }
                }
            }
        } else {
            // Not a select path. Recurse into groups.
            match &tts[i] {
                TokenTree::Group(group) => {
                    let inner: Vec<TokenTree> = group.stream().into_iter().collect();
                    let processed = process_tokens(&inner);
                    let new_stream: TokenStream = processed.into_iter().collect();
                    let mut new_group = Group::new(group.delimiter(), new_stream);
                    new_group.set_span(group.span());
                    out.push(TokenTree::Group(new_group));
                }
                other => out.push(other.clone()),
            }
            i += 1;
        }
    }

    out
}

/// Check if tokens starting at `start` form `crossbeam_channel :: select !`
/// or `crossbeam_channel :: select_biased !`.
///
/// Returns (number_of_tokens_consumed, true) on match, or (0, false).
fn match_crossbeam_select_path(tts: &[TokenTree], start: usize) -> (usize, bool) {
    // Need at least: crossbeam_channel :: select/select_biased !
    // That's: Ident `:` `:` Ident `!` = 5 tokens
    if start + 4 >= tts.len() {
        return (0, false);
    }

    // Token 0: `crossbeam_channel`
    let is_crossbeam = matches!(&tts[start], TokenTree::Ident(id) if *id == "crossbeam_channel");
    if !is_crossbeam {
        return (0, false);
    }

    // Tokens 1-2: `::`
    if !is_double_colon(tts, start + 1) {
        return (0, false);
    }

    // Token 3: `select` or `select_biased`
    let is_select_ident = matches!(
        &tts[start + 3],
        TokenTree::Ident(id) if *id == "select" || *id == "select_biased"
    );
    if !is_select_ident {
        return (0, false);
    }

    // Token 4: `!`
    let is_bang = matches!(&tts[start + 4], TokenTree::Punct(p) if p.as_char() == '!');
    if !is_bang {
        return (0, false);
    }

    (5, true)
}

/// Check if tokens[i] and tokens[i+1] form `::`.
fn is_double_colon(tts: &[TokenTree], i: usize) -> bool {
    if i + 1 >= tts.len() {
        return false;
    }
    matches!(
        (&tts[i], &tts[i + 1]),
        (
            TokenTree::Punct(a),
            TokenTree::Punct(b),
        ) if a.as_char() == ':' && a.spacing() == Spacing::Joint && b.as_char() == ':'
    )
}

/// Check if tokens[i] and tokens[i+1] form `->`.
fn is_thin_arrow(tts: &[TokenTree], i: usize) -> bool {
    if i + 1 >= tts.len() {
        return false;
    }
    matches!(
        (&tts[i], &tts[i + 1]),
        (
            TokenTree::Punct(a),
            TokenTree::Punct(b),
        ) if a.as_char() == '-' && a.spacing() == Spacing::Joint && b.as_char() == '>'
    )
}

use super::token_util::is_fat_arrow;

/// The kind of select arm (recv or send).
#[derive(Clone, Copy)]
enum ArmKind {
    Recv,
    Send,
}

/// Parsed select arm: the operation kind, channel ident, result ident, and
/// the index of the body group in the token array.
struct SelectArm {
    kind: ArmKind,
    channel_ident: String,
    result_ident: String,
    body_index: usize,
}

/// Scan tokens inside a select! body for recv/send arms and inject stats calls.
fn inject_into_select_body(tts: &[TokenTree]) -> Vec<TokenTree> {
    // First, identify all arms so we know which body groups to modify.
    let arms = find_select_arms(tts);

    // Rebuild the token stream, modifying body groups at arm positions.
    // Linear scan is appropriate: select! typically has 1-3 arms.
    let mut out: Vec<TokenTree> = Vec::with_capacity(tts.len());
    for (i, tt) in tts.iter().enumerate() {
        if let Some(arm) = arms.iter().find(|a| a.body_index == i) {
            if let TokenTree::Group(group) = tt {
                if group.delimiter() == Delimiter::Brace {
                    let method = match arm.kind {
                        ArmKind::Recv => "record_recv_if_ok",
                        ArmKind::Send => "record_send_if_ok",
                    };
                    // Recurse into the body to handle nested select! invocations.
                    let body_tts: Vec<TokenTree> = group.stream().into_iter().collect();
                    let body_tokens: TokenStream = process_tokens(&body_tts).into_iter().collect();
                    let prepended = prepend_record_call(
                        body_tokens,
                        &arm.channel_ident,
                        &arm.result_ident,
                        method,
                    );
                    let mut new_group = Group::new(Delimiter::Brace, prepended);
                    new_group.set_span(group.span());
                    out.push(TokenTree::Group(new_group));
                    continue;
                }
            }
        }
        out.push(tt.clone());
    }

    out
}

/// Find all recv/send arms in a select! body's token stream.
///
/// Arm pattern: `recv(channel) -> result => { body }`
/// or:          `send(channel, value) -> result => { body }`
fn find_select_arms(tts: &[TokenTree]) -> Vec<SelectArm> {
    let mut arms = Vec::new();
    let mut i = 0;

    while i < tts.len() {
        // Try to match `recv` or `send` ident.
        let arm_kind = match &tts[i] {
            TokenTree::Ident(id) => match id.to_string().as_str() {
                "recv" => Some(ArmKind::Recv),
                "send" => Some(ArmKind::Send),
                _ => None,
            },
            _ => None,
        };

        if let Some(kind) = arm_kind {
            if let Some(arm) = try_parse_arm(tts, i, kind) {
                let next = arm.body_index + 1;
                arms.push(arm);
                // Skip past the arm to avoid re-matching.
                i = next;
                continue;
            }
        }

        i += 1;
    }

    arms
}

/// Try to parse a select arm starting at `start` which points to a
/// `recv` or `send` ident.
///
/// Expected pattern: `recv ( channel ) -> result => { body }`
///                or `send ( channel , value ) -> result => { body }`
///
/// Returns the parsed arm if the pattern matches.
fn try_parse_arm(tts: &[TokenTree], start: usize, kind: ArmKind) -> Option<SelectArm> {
    let mut i = start + 1;

    // Next: parenthesized group containing the channel (and optionally value for send).
    if i >= tts.len() {
        return None;
    }
    let channel_ident = match &tts[i] {
        TokenTree::Group(group) if group.delimiter() == Delimiter::Parenthesis => {
            // First ident in the group is the channel name.
            let inner: Vec<TokenTree> = group.stream().into_iter().collect();
            match inner.first() {
                Some(TokenTree::Ident(id)) => id.to_string(),
                _ => return None,
            }
        }
        _ => return None,
    };
    i += 1;

    // Next: `->` (thin arrow)
    if !is_thin_arrow(tts, i) {
        return None;
    }
    i += 2;

    // Next: result ident
    if i >= tts.len() {
        return None;
    }
    let result_ident = match &tts[i] {
        TokenTree::Ident(id) => id.to_string(),
        _ => return None,
    };
    i += 1;

    // Next: `=>` (fat arrow)
    if !is_fat_arrow(tts, i) {
        return None;
    }
    i += 2;

    // Next: body group `{ ... }`
    if i >= tts.len() {
        return None;
    }
    match &tts[i] {
        TokenTree::Group(group) if group.delimiter() == Delimiter::Brace => Some(SelectArm {
            kind,
            channel_ident,
            result_ident,
            body_index: i,
        }),
        _ => None,
    }
}

/// Build `channel . stats . method ( & result ) ;` and prepend it to the body.
fn prepend_record_call(
    body: TokenStream,
    channel_ident: &str,
    result_ident: &str,
    method: &str,
) -> TokenStream {
    let span = Span::call_site();

    let channel = Ident::new(channel_ident, span);
    let dot1 = Punct::new('.', Spacing::Alone);
    let stats = Ident::new("stats", span);
    let dot2 = Punct::new('.', Spacing::Alone);
    let method_ident = Ident::new(method, span);
    let amp = Punct::new('&', Spacing::Alone);
    let result = Ident::new(result_ident, span);
    let semi = Punct::new(';', Spacing::Alone);

    // Build the argument group: `( & result )`
    let arg_tokens: TokenStream = vec![TokenTree::Punct(amp), TokenTree::Ident(result)]
        .into_iter()
        .collect();
    let args = Group::new(Delimiter::Parenthesis, arg_tokens);

    let mut prefix: Vec<TokenTree> = vec![
        TokenTree::Ident(channel),
        TokenTree::Punct(dot1),
        TokenTree::Ident(stats),
        TokenTree::Punct(dot2),
        TokenTree::Ident(method_ident),
        TokenTree::Group(args),
        TokenTree::Punct(semi),
    ];

    // Append the original body tokens.
    prefix.extend(body);

    prefix.into_iter().collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn inject_and_stringify(input: &str) -> String {
        let tokens: TokenStream = input.parse().unwrap();
        let result = inject_select_arm_stats(tokens);
        result.to_string()
    }

    #[test]
    fn injects_into_recv_arm_body() {
        let input = r#"crossbeam_channel::select! {
            recv(rx) -> msg => {
                process(msg)
            }
        }"#;
        let output = inject_and_stringify(input);
        assert!(
            output.contains("record_recv_if_ok"),
            "should inject record_recv_if_ok: {output}"
        );
        assert!(
            output.contains("process"),
            "should preserve original body: {output}"
        );
    }

    #[test]
    fn injects_into_send_arm_body() {
        let input = r#"crossbeam_channel::select! {
            send(tx, value) -> res => {
                handle(res)
            }
        }"#;
        let output = inject_and_stringify(input);
        assert!(
            output.contains("record_send_if_ok"),
            "should inject record_send_if_ok: {output}"
        );
        assert!(
            output.contains("handle"),
            "should preserve original body: {output}"
        );
    }

    #[test]
    fn does_not_inject_into_default_arm() {
        let input = r#"crossbeam_channel::select! {
            recv(rx) -> msg => { process(msg) }
            default => { fallback() }
        }"#;
        let output = inject_and_stringify(input);
        let count = output.matches("record_recv_if_ok").count();
        assert_eq!(
            count, 1,
            "should only inject into recv arm, not default: {output}"
        );
    }

    #[test]
    fn handles_multiple_recv_arms() {
        let input = r#"crossbeam_channel::select! {
            recv(rx1) -> msg => { one(msg) }
            recv(rx2) -> msg => { two(msg) }
            recv(rx3) -> msg => { three(msg) }
        }"#;
        let output = inject_and_stringify(input);
        let count = output.matches("record_recv_if_ok").count();
        assert_eq!(count, 3, "should inject into all 3 recv arms: {output}");
    }

    #[test]
    fn handles_select_biased() {
        let input = r#"crossbeam_channel::select_biased! {
            recv(rx) -> msg => { process(msg) }
        }"#;
        let output = inject_and_stringify(input);
        assert!(
            output.contains("record_recv_if_ok"),
            "should handle select_biased!: {output}"
        );
    }

    #[test]
    fn ignores_non_crossbeam_select() {
        let input = r#"tokio::select! {
            msg = rx.recv() => { process(msg) }
        }"#;
        let output = inject_and_stringify(input);
        assert!(
            !output.contains("record_recv_if_ok"),
            "should not inject into tokio::select!: {output}"
        );
    }

    #[test]
    fn preserves_non_select_code() {
        let input = r#"let x = 42; println!("{}", x);"#;
        let output = inject_and_stringify(input);
        assert!(
            !output.contains("record_recv_if_ok"),
            "should not modify non-select code"
        );
    }

    #[test]
    fn handles_mixed_recv_and_send_arms() {
        let input = r#"crossbeam_channel::select! {
            recv(rx) -> msg => { process(msg) }
            send(tx, val) -> res => { handle(res) }
        }"#;
        let output = inject_and_stringify(input);
        assert_eq!(
            output.matches("record_recv_if_ok").count(),
            1,
            "should inject recv stats: {output}"
        );
        assert_eq!(
            output.matches("record_send_if_ok").count(),
            1,
            "should inject send stats: {output}"
        );
    }

    #[test]
    fn injected_form_is_exact() {
        let input = r#"crossbeam_channel::select! {
            recv(rx) -> msg => { process(msg) }
        }"#;
        let output = inject_and_stringify(input);
        assert!(
            output.contains("rx . stats . record_recv_if_ok (& msg) ;"),
            "injected call should be `rx . stats . record_recv_if_ok (& msg) ;`: {output}"
        );
    }

    #[test]
    fn nested_select_in_arm_body() {
        let input = r#"crossbeam_channel::select! {
            recv(rx1) -> msg => {
                crossbeam_channel::select! {
                    recv(rx2) -> inner => { handle(inner) }
                }
            }
        }"#;
        let output = inject_and_stringify(input);
        assert_eq!(
            output.matches("record_recv_if_ok").count(),
            2,
            "should inject into both outer and nested select! recv arms: {output}"
        );
    }
}

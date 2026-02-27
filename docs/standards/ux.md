# UX Principles

These principles are design constraints for every user-facing decision in Piano.
They apply to CLI flags, error messages, output formatting, defaults, and help text.

## The Lens

For every user-facing touchpoint, ask: does this force the user to think about
Piano instead of their actual problem?

Frictionless is not "feels like another tool." It is "feels like nothing." The
user does not notice the tool -- they think the thing and the thing happens.

## Principles

### 1. Honor existing affordances

The user arrives with knowledge from shell, cargo, git, and every other CLI they
have used. Piano must honor that knowledge, not compete with it. If the user
already knows what "self time" means from every other profiler, Piano calls it
"self time." If substring matching is the convention for filtering named things
(cargo test, pytest -k, go test -run), Piano uses substring matching.

When Piano invents a convention, it creates a learning moment. Every learning
moment is potential friction.

Corollary: never let this sentence happen: "everyone does it one way, except
Piano."

### 2. Never duplicate affordances

Each visual channel, output element, and interaction communicates one thing that
no other element already communicates. If position conveys "this is the hot
function" (sort order), color must not also convey "this is the hot function."
Use color for something position cannot express (report structure, section
boundaries).

Wasting an affordance on repetition means something else goes unsaid.

### 3. Never afford the wrong user

When a feature is not active, it is invisible. No jargon, no output, no footer
text from an inactive feature should leak into the default experience. The
default report is for the frustrated Googler who does not know profiling. Advanced
features (frames, percentiles, spike detection) appear only when explicitly
requested.

### 4. Errors define what is valid

Error messages redirect energy toward correct usage, not away from incorrect
usage. "Valid tags cannot include slashes" not "tag name must not contain path
separators." Show what works, not what failed. The user should leave an error
message knowing what to type next.

### 5. Optimization is violent from the inside, peaceful from the outside

Do not band-aid slow operations with progress bars. Make the operation fast
enough that the user would never think to ask for one. Gut internals, ensure
correctness, and the user experiences a faster program. Only then evaluate
whether feedback is still needed.

### 6. When the profiled program errors, Piano gets out of the way

The user's program error is the primary affordance. Piano must not cascade its
own errors on top (e.g., "no run file found" when the program crashed before
producing data). One problem, one error. The user's error takes priority.

### 7. Affordances are finite resources

Every piece of information shown to the user occupies attention. If the user
cannot act on the information (run_id they cannot pass to any command, internal
path they cannot navigate to), it is noise, not an affordance. Remove it.

Before adding output, ask: can the user do anything with this? If no, do not
show it.

### 8. Never vary an affordance by quantity

The same situation gets the same treatment regardless of scale. A warning that
shows function names inline for 3 items but hides them behind a flag for 20 is
two behaviors for the same situation. The user has to learn both. Pick one format
and apply it at any count.

### 9. Make failure impossible, not handled

If a case should not happen, restructure the internals so it cannot. Do not write
graceful error handling for failures your architecture should prevent. Handling a
failure that should not exist is admitting the architecture allows it. Eliminate
the case, do not catch it.

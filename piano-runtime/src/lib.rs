mod collector;

pub use collector::{
    AdoptGuard, FunctionRecord, Guard, SpanContext, adopt, collect, enter, flush, fork, init,
    register, reset,
};

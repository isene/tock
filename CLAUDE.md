# Tock

Rust feature clone of [Timely](https://github.com/isene/timely), a terminal calendar app.

TUI calendar with day/week/month views, event management, and astrological data. Built on Crust.

## Build

```bash
PATH="/usr/bin:$PATH" cargo build --release
```

Note: `PATH` prefix needed to avoid `~/bin/cc` (Claude Code sessions) shadowing the C compiler.

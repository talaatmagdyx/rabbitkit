# Migration guide

Breaking changes and deprecations to Stable Core / Advanced Stable APIs,
one entry per change, in the order they happened. See
[`docs/stability-policy.md`](stability-policy.md) for the deprecation
policy this follows. Experimental API changes are not tracked here — see
`CHANGELOG.md` for those.

## `rabbitkit.aio` → `rabbitkit.async_` (1.1.0)

**What changed:** `rabbitkit.async_` is now the canonical import path for
the async broker and transport. `rabbitkit.aio` still works, but importing
it now emits a `DeprecationWarning`.

**Why:** both paths existed with no documented canonical answer. Usage
across the codebase and documentation was already overwhelmingly
`rabbitkit.async_` (roughly 4:1), so that became the canonical path rather
than picking one arbitrarily.

**Before:**

```python
from rabbitkit.aio import AsyncBroker
```

**After:**

```python
from rabbitkit.async_ import AsyncBroker
# or, equivalently, from the top level:
from rabbitkit import AsyncBroker
```

**Timeline:** deprecated in `1.1.0`. Per the deprecation policy, it will be
removed no earlier than the following minor release. If you see the
`DeprecationWarning`, update the import now — there's no behavior
difference, `rabbitkit.aio` re-exports the exact same class.

**How to check if you're affected:**

```bash
grep -rn "from rabbitkit.aio\|from rabbitkit import aio\|import rabbitkit.aio" .
```

---

## Template for future entries

When a Stable Core or Advanced Stable API changes in a way that requires
user action, add an entry here following this shape:

```markdown
## <old symbol/path> → <new symbol/path> (<version>)

**What changed:** ...
**Why:** ...
**Before:** <code>
**After:** <code>
**Timeline:** deprecated in `X.Y.Z`; removed no earlier than the following minor release.
**How to check if you're affected:** <a grep/search command, if applicable>
```

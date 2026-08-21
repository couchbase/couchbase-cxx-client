# Writing tests for couchbase-cxx-client

This is the standard every test in this directory is held to, and what a reviewer checks a test diff against. It is short on ceremony and long on the handful of mistakes that produce a test which cannot fail — because those are the expensive ones, and every one listed here has actually been made in this repository.

Two suites live here. The hand-rolled framework in `framework/` is the one to write new tests against; Catch2 is being removed (CXXCBC-945) and no new file should use it. `cng/` holds the `couchbase2://` tests and has its own [README](cng/README.md) for standing up a gateway.

## Contents

- [What a test is for](#what-a-test-is-for)
- [The order that produces a real test](#the-order-that-produces-a-real-test)
- [Negative control: watch it fail](#negative-control-watch-it-fail)
- [Choosing the venue](#choosing-the-venue)
- [Assert the thing that distinguishes](#assert-the-thing-that-distinguishes)
- [Preconditions and postconditions](#preconditions-and-postconditions)
- [Never drop a return value](#never-drop-a-return-value)
- [A skip is a claim](#a-skip-is-a-claim)
- [Determinism](#determinism)
- [Debuggability](#debuggability)
- [Compile time](#compile-time)
- [Comments in tests](#comments-in-tests)
- [Mechanics](#mechanics)
- [Reviewer's checklist](#reviewers-checklist)

## What a test is for

A test **specifies** correct behaviour. It does not record current behaviour, and it does not pin an internal detail that is free to change.

The distinction is not academic. A test written by running the code and asserting whatever came out is a photograph: it will fail when the code is improved and pass when the code is wrong in a way the photograph happened to capture. Ask, before writing the assertion, what the operation is *supposed* to do — from the RFC, the public API contract, or the server's documented semantics — and assert that.

A test that pins an internal is worse than none: it makes a correct refactor look like a regression, and the pressure is then on the refactor.

## The order that produces a real test

1. Decide what correct behaviour is.
2. Write the assertion.
3. **Watch it fail** against the current (or missing) code.
4. Implement.
5. Watch it pass.

Step 3 is the one that gets skipped, and it is the only one that proves anything. A test that passes the moment it is written has demonstrated that it passes — not that it can fail.

## Negative control: watch it fail

For a test added alongside a fix, the control is: revert the fix, rebuild, observe **the named assertion** fail, restore. For a test added to existing code, mutate the code it claims to cover.

There are four outcomes, and **three of them look like success at a glance**:

| outcome | meaning |
|---|---|
| **killed** | the mutation was applied, the build succeeded, the named assertion failed — the only outcome that is evidence |
| **survived** | applied and built, but nothing failed — the test does not cover what you thought |
| **not applied** | the edit did not match; nothing was mutated and nothing was tested |
| **build failed** | the mutated tree did not compile; nothing was tested |

**Confirm the control build succeeded before reading its result.** Reverting a parameter's only use once left it unused, `-Werror=unused-parameter` failed the build, the build system kept the previous binary, and the "control" passed against stale code — making the fix look unnecessary.

**Neuter the value, not the use.** Replacing a parameter's only use with a literal leaves it unused and fails the build. Multiply a count by zero; substitute `(static_cast<void>(x), T{})` for a container.

**Nothing else may build the tree while a control is applied**, including anything that snapshots the working copy — a snapshot taken mid-mutation commits mutated source.

If a case is new coverage rather than a regression test, say so. A case that passes either way is not proof of anything, and calling it one is how a fix ships unverified.

## Choosing the venue

A live server is the strongest venue for *server* semantics and the weakest for *client* choices. Where the server would produce the same observable as the bug, a live case cannot fail — and it still looks like coverage. Decide per property, not per operation.

| the property is about | venue | what you assert |
|---|---|---|
| what the server does with what was sent | a real cluster | the server's own answer |
| which request the client chose to send, or whether it sent one | an in-process recording server | the **call list**, not the error code |
| a pure transform | a unit test | both directions, with distinct payloads |

Two worked examples, both of which passed review and were caught only by a mutation sweep:

- **"An empty index name is refused."** The client refuses locally with `invalid_argument`; the gateway *also* answers `InvalidArgument`. Removing the client-side guard changed nothing observable. Rewritten against a server that records method names, the assertion became "the call list is empty" — which is the actual claim, and which fails the moment the guard goes.
- **"A response with no index yields `index_not_found`."** Not testable live at all: a real server never sends that shape. The mutation came back *not applied*, i.e. the case was never evaluated.

Where a property genuinely cannot be checked anywhere, write that down rather than letting a case count imply coverage that does not exist.

**Make a deliberate refusal falsifiable.** Asserting that an unsupported operation returns `feature_not_available` proves nothing on its own: a deployment with no such service answers identically. Call a neighbouring operation first and require it to succeed; only then assert the refusal.

## Assert the thing that distinguishes

An error code often cannot separate two very different stories. *Rejected before dispatch* and *dispatched, then failed* both surface as a timeout. Only a server-side call counter tells them apart.

So give the test double the knobs that make the distinction observable — a reply delay and a `calls_received` counter cover every timeout case in this suite — and assert on those, not on the code that both stories produce.

## Preconditions and postconditions

**State the preconditions.** A case that assumes a document exists, a collection is empty, or a previous case left something behind is a case that passes for a reason other than the one it names. Set up what the case needs, or declare it as a requirement.

**Assert the postconditions in full, including what must not have changed.** An upsert test that checks the new value and not the CAS, the expiry, or the untouched sibling field has tested a third of the operation. When a test says "this field is updated", it is also claiming the others are not — assert that claim.

## Never drop a return value

Every result is checked. Every one.

```cpp
auto [err, result] = collection.get(id, {}).get();
assert_success(err, "the document is readable");
assert_eq(result.content_as<std::string>(), expected, "and carries what was written");
```

not

```cpp
collection.get(id, {}).get();   // no
```

A dropped `std::error_code` is precisely the class of defect this suite exists to catch, and a test that drops one is asserting nothing while occupying a line in the report. The framework marks its accessors `[[nodiscard]]` so that ignoring one is a compile error rather than a review comment.

## A skip is a claim

A case that declares `{ needs::service("n1ql") }` is claiming that the requirement is *sometimes* satisfied and *sometimes* not. Both halves matter:

- A requirement **never satisfied** anywhere in the CI matrix means the case has never run. It reports nothing and is indistinguishable from a case that passes.
- A requirement **never unsatisfied** is dead weight: it costs a probe and gates nothing.

The runner prints, at the end of every run, the cases a **requirement** turned away, grouped by what was missing. A `skip()` called from a case body is counted in the totals but not grouped here, because there is no requirement to group it under:

```
Skipped for want of:
  the n1ql service — 32 case(s)
  a cluster at 7.6.0 or later — 4 case(s)
```

Read it. A line you did not expect is either a gap in the environment or a case that has quietly stopped running.

Requirements are declared beside the registration, never checked inside the body — see [Mechanics](#mechanics). A guard inside a body is invisible to `--list-tests`, absent from that summary, and forced to answer "I could not find out" with either *yes* or *no*.

## Determinism

**No fixed sleep standing in for a condition.** `sleep_for(500ms)` is a guess that is too short on a loaded CI machine and too long on every other run. Poll the condition, or use a mutation token, or wait on the thing you actually mean.

**Join threads before asserting.** A failed assertion throws, and unwinding past a joinable `std::thread` calls `std::terminate` — replacing the assertion message with a bare `terminate called`, which says nothing about what failed. Tear down, then assert.

**No dependence on case order.** Cases in a binary share a process, and the runner may share a cluster connection between them; it may also be told not to. A case that only passes after its neighbour has run is a case that will fail alone, under `-R`, which is how anybody debugs it.

**Budgets are per case, and they are real.** Pick a `timeout::` preset by what the case waits for, not by what number looks safe. A case that exceeds its budget is counted as a failure and additionally flagged as a timeout. The runner cannot kill it: it stops waiting and leaves the worker detached and still running, which is why `main()` leaves through `_Exit` rather than running static destructors underneath it.

## Debuggability

**One case, one behaviour.** A case that asserts six unrelated things reports the first failure and hides the rest, and its name cannot describe what it does.

**The name states the claim.** `wan_development_sets_every_timeout_it_owns`, not `test_profile_2`. The name is what a failing CI job shows, and it should be enough to know what broke without opening the file.

**The name is an identifier, not a sentence.** `CASE(fn)` stringifies the function's own name, so a case name is `[A-Za-z_][A-Za-z0-9_]*` in lower_snake_case and nothing else — no spaces, no colons, no punctuation. That is what lets the name pass untouched through a CMake list, a `ctest -R` pattern and a shell argv on every platform, with no quoting anywhere. Do not spell a category into it: `integration:` is carried by the target's ctest label and by the case's requirements. A case migrated from Catch2 is converted to an identifier once, when it is migrated — `"integration: cluster remains usable in a forked child"` becomes `cluster_remains_usable_in_a_forked_child` — and if a name will not convert, rename the case rather than teaching anything downstream to escape it.

### The assertion vocabulary

| assertion | holds when | prints on failure |
|---|---|---|
| `assert_true(v)` / `assert_false(v)` | `v` is true / false | the message only; there is no operand worth printing |
| `assert_eq(a, b)` / `assert_ne(a, b)` | `a == b` / `a != b` | both operands, or `both are: <v>` for `assert_ne` |
| `assert_contains(haystack, needle)` | `needle` is a substring | `"<needle>" is not in "<haystack>"` |
| `assert_starts_with(s, prefix)` | `s` begins with `prefix` | `"<s>" does not start with "<prefix>"` |
| `assert_near(a, b, tol)` | the difference is within `tol` | `actual: <a>, expected: <b> ± <tol>` |
| `assert_no_throw(fn)` | `fn()` throws nothing | the exception's `what()` |
| `assert_throws<E>(fn)` | `fn()` throws an `E` | what was thrown instead, or that nothing was |
| `assert_success(err)` / `assert_error(err, ec)` | from `framework/errors.hxx` | the error's category, value, message, and for `couchbase::error` its context and cause chain |
| `fail(why)` | never — an unreachable branch | `why` |
| `skip(why)` | never — the case does not apply | `why`, and the case is counted as skipped |

`assert_near` takes `double`. A `float` operand promotes to it and needs no cast. A `long double` one does not fit, and both GCC and clang reject it under this project's warning flags — cast it at the call site, where the loss of precision is visible.

Prefer a requirement over `skip()`: a requirement is declared beside the case, is visible in `--list-tests`, and is grouped in the run summary. `skip()` is for the rare precondition no requirement can express.

**The failure message names the operands.** `assert_eq` and `assert_ne` print both sides; `assert_success` prints the error's category, value, message, and for `couchbase::error` its context and cause chain. Where you write a message yourself, state what the assertion *means*:

```cpp
assert_eq(opts.key_value_timeout.count(), 20'000, "key/value timeout");              // yes
assert_eq(opts.key_value_timeout.count(), 20'000, "check key_value_timeout==20000"); // no
```

**Cases are ordinary functions.** No macro wraps a case body, so go-to-definition finds it, a breakpoint sticks in it, and a debugger names it.

## Compile time

Include the lightest header that does the job. Six unit files currently include `test_helper_integration.hxx` — dragging in `core/cluster.hxx` and `core/operations.hxx` — without ever constructing a cluster.

`framework/test_registry.hxx` is what a test file needs, and it deliberately reaches nothing heavy: no formatting library, no `core/`, no `couchbase/`. `framework/errors.hxx` is separate because it names `couchbase::error`, and a file that does not assert on one should not pay for it. `framework/test_runner.hxx` is framework internals; a test does not include it.

**Assertion messages are string literals.** Across the whole suite, five call sites out of about 1,370 build a message from values, and the framework is arranged so those five pay for it rather than everybody:

```cpp
assert_eq(actual, expected, "the value survives the round trip");        // yes: a literal
assert_eq(actual, expected, fmt::format("round trip of {}", key));       // only if you must
```

The failure message already prints the operands — that is what `operand_printer` is for — so building one by hand is nearly always redundant. If a file genuinely needs it, that file includes `<spdlog/fmt/fmt.h>` itself.

### Printing a type in a failure message

`assert_eq` and `assert_ne` show both operands for any type with an `operand_printer`. Integers, `bool`, `char`, floating point, the string types and enums are built in; `framework/errors.hxx` adds `std::error_code` and `couchbase::error`. A type with none is not an error — the assertion still fires, it just omits the values. To teach the framework a type, specialise it next to the test that needs it:

```cpp
template<>
struct couchbase::test::operand_printer<my_type> {
  static constexpr bool available = true;
  [[nodiscard]] static auto to_text(const my_type& value) -> std::string
  {
    return value.name();
  }
};
```

## Comments in tests

**The assertion is the specification.** The comment names the regression the assertion catches, and stops:

```cpp
// A durable write with a replica count above the cluster's must be refused before dispatch, not
// after a timeout: CXXCBC-904 shipped the second behaviour and it looked like a slow cluster.
assert_error(err, errc::key_value::durability_impossible, "refused before dispatch");
```

Not a tour of the implementation, not a restatement of the code in English, and not what the code used to do.

**A comment that contradicts the code beneath it is a false specification**, not untidiness. Reviewers and later changes act on it. When either side changes, change both.

## Mechanics

### Registering a case

```cpp
#include "framework/test_registry.hxx"

namespace couchbase::test
{
namespace
{
void
a_sentence_about_what_is_pinned([[maybe_unused]] context& ctx)
{
  assert_eq(subject.value(), 42, "the value survives the round trip");
}
} // namespace

auto
tests() -> test_suite
{
  return {
    suite_name,
    {
      { CASE(a_sentence_about_what_is_pinned) },
      { CASE(needs_a_server), { needs::real_cluster() }, timeout::integration },
    },
  };
}

} // namespace couchbase::test
```

`CASE(fn)` expands to the case name and the function pointer. Never write the name out separately — a typo in the string compiles, and that string is both the ctest entry and the filter key. `suite_name` comes from the path CMake registered the file under.

Register the file in `test/CMakeLists.txt`:

```cmake
couchbase_add_test(unit/core/config_profiles LABEL unit LINK_CLIENT)
```

`LABEL` is one of `unit`, `integration`, `transaction`, `benchmark`, `cng`, and decides which CI leg runs it. Every one of them is selected by a leg in `bin/` or `.github/workflows/`; a label no leg selects would build and link its cases and never run them. The framework's own selftests carry `cng`. `LINK_CLIENT` links the client library and the cluster-backed probes; omit it for a test that needs neither. `LIBS` adds anything further.

### Declaring what a case needs

| requirement | satisfied when |
|---|---|
| `needs::real_cluster()` | `TEST_CONNECTION_STRING` is set and the mock is not in use |
| `needs::mock()` | the gocaves mock is in use |
| `needs::service("n1ql")` | the cluster runs a node with that service (`kv`, `n1ql`, `index`, `fts`, `cbas`, `eventing`) |
| `needs::cluster_version(v7_6)` | the cluster is at least that release |
| `needs::cluster_version(v7_0, v7_6)` | the cluster is in `[7.0, 7.6)` — half open |
| `needs::bucket_capability("durableWrite")` | the bucket reports it |
| `needs::storage_backend("magma")` | the bucket uses that backend |
| `needs::replicas(2)`, `needs::nodes(3)`, `needs::server_groups(2)` | at least that many |
| `needs::edition(server_edition::enterprise)` | the cluster reports that edition |
| `needs::deployment(deployment_type::capella)` | the deployment matches |
| `needs::developer_preview()` | developer preview is on |

Each is checked **before** the case runs, and yields one of three answers:

- **satisfied** — the case runs.
- **not satisfied** — the case is skipped, and the reason appears in the run's summary.
- **undetermined** — the case **fails**. A cluster that is configured but cannot answer is not the same as no cluster, and only one of those is a reason to skip.

### Writing your own requirement

Anything the vocabulary does not cover goes in the file that needs it. No framework change:

```cpp
class needs_even_replicas : public requirement
{
public:
  [[nodiscard]] auto describe() const -> std::string override
  {
    return "an even number of replicas";
  }
  [[nodiscard]] auto check(context& ctx) const -> check_result override
  {
    return ctx.number_of_replicas() % 2 == 0 ? check_result::ok()
                                             : check_result::missing("the count is odd");
  }
};
```

Register it with `std::make_shared<const needs_even_replicas>()`. A probe that cannot answer throws, the runner turns that into *undetermined*, and the requirement handles no errors of its own.

### Sharing

Each ctest entry runs its own process, so a case is isolated from its neighbours by construction. Nothing is shared between cases today; when a connection eventually is, sharing will be the **runner's** decision rather than something each case declares.

### Running

```console
$ ctest --test-dir build -L unit                       # one label
$ ctest --test-dir build -R 'unit_core_config_profiles' # one binary
$ ctest --test-dir build -R 'unit_core_config_profiles\.an_unknown_profile_name_raises'
$ ./build/test/unit_core_config_profiles               # the binary directly, all cases
$ ./build/test/unit_core_config_profiles an_unknown_profile_name_raises
$ ./build/test/unit_core_config_profiles --list-tests   # every case, and what it requires
```

Each case is a separate ctest entry, so `-R` selects one, `-I i,,n` shards the suite, and the JUnit report has a row per case.

Exit codes: **0** pass, **1** failure, **77** every case was skipped (ctest reports *Skipped*). A binary that ran nothing at all exits 1 — a suite that verified nothing must not read as green.

`CB_TEST_TIMEOUT_MULTIPLIER` scales every case budget, for a run under valgrind or a sanitizer. `bin/run-integration-tests` sets it for those legs.

### Environment

A case reads its configuration from `ctx.config()`, not from `getenv`:

| variable | reaches |
|---|---|
| `TEST_CONNECTION_STRING` | `config().connection_string`, and `config().cluster_configured` |
| `TEST_USERNAME`, `TEST_PASSWORD` | `config().username`, `config().password` |
| `TEST_BUCKET`, `TEST_OTHER_BUCKET` | `config().bucket`, `config().other_bucket` |
| `TEST_USE_GOCAVES` / `CB_USE_GOCAVES` | `config().mock` |

`ctx.env("SOMETHING")` exists for the rare case that genuinely needs a variable of its own.

## Reviewer's checklist

Every item is checkable against a diff.

- [ ] Each case name is a sentence about behaviour, not a number or a noun.
- [ ] Each case asserts one behaviour.
- [ ] Every assertion carries a message saying what it means.
- [ ] No return value is dropped.
- [ ] Postconditions include what must **not** have changed.
- [ ] Preconditions are established or declared, not assumed.
- [ ] Every environmental gate is a requirement at the registration, not an `if` in the body.
- [ ] The venue can distinguish the failure — a client-side choice is asserted against a recording server, not a live one.
- [ ] A refusal case proves the service is alive first.
- [ ] No fixed sleep stands in for a condition; threads are joined before assertions.
- [ ] Timeouts use a `timeout::` preset chosen by what the case waits for.
- [ ] The lightest sufficient headers are included.
- [ ] Comments state the rule, and agree with the code beneath them.
- [ ] The author states how each new assertion was observed failing.

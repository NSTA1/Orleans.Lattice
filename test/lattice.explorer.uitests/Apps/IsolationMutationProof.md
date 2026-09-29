# Mutation-test record: frame isolation

This file records the mutation evidence for `Apps/AppFrameIsolationTests`, the epic's
named security test (#3807, E4 and E5), taken with this suite (issue #3832) in Chromium,
Firefox and WebKit.

## 1. `allow-same-origin` on the frame element alone - still contained

The mutation adds `allow-same-origin` to the frame in
`src/lattice.explorer/UI/AppFrame/AppFrame.razor`:

```razor
sandbox="allow-scripts allow-same-origin"
```

Result: the `exfiltrate`, `storage` and `navigate-out` cases **passed** in all three
engines (9 of 9).

That is not a blind spot. The bootstrap document is also served with the E4 policy
(`AppFrameRoute.ContentSecurityPolicyText`), whose first directive is
`sandbox allow-scripts`, and a CSP sandbox is applied on top of the element's: the
document stays an opaque origin, so the storage, cookie and parent probes are still
refused. The element attribute and the response header are two independent locks, and
removing one leaves the frame locked.

## 2. Both locks removed - every engine goes red

The mutation adds `allow-same-origin` to the element as above and also removes
`sandbox allow-scripts; ` from `AppFrameRoute.ContentSecurityPolicyText`, so the frame
document becomes same-origin with the Explorer.

Result: the `storage` and `exfiltrate` cases **failed** in all three engines (6 of 6):

```
Failed A_hostile_bundle_is_contained("storage")      Expected: "local=blocked"   But was: "local=allowed"
Failed A_hostile_bundle_is_contained("exfiltrate")   Expected: "parent=blocked"  But was: "parent=allowed"
```

Each hostile bundle reports what it managed through the bridge, and the case compares every
attempt; one attempt that gets through is enough to fail it.

## Why no case can pass vacuously

A case whose bundle must run waits for the report only its own script can send, and fails
naming the bundle when the report never comes. The two failure cases that must never run
(`entry-script`, `digest-tamper`) assert that no frame element exists at all. The
`navigate-out` case first waits for the head to receive the frame's request, so it proves
the navigation was attempted before it proves the page never rendered.

## How to reproduce

1. Apply one of the mutations above.
2. `dotnet build test/lattice.explorer.uitests/Orleans.Lattice.Explorer.UiTests.csproj -c Release`
3. `dotnet test test/lattice.explorer.uitests/Orleans.Lattice.Explorer.UiTests.csproj -c Release --no-build --filter "FullyQualifiedName~AppFrameIsolationTests&(FullyQualifiedName~exfiltrate|FullyQualifiedName~storage|FullyQualifiedName~navigate-out)"`
4. Restore the source and confirm `git diff src/` is clean.

# Password Protection

> This sample shows the built-in **username/password** authentication front door
> for the State API gRPC surface (`AddEnvVarCredentialAuthorizer`) composed with
> the per-tree **authorization** layer (`AddLatticeMembership` + `AddLatticeAuth`)
> on a single silo. For the authorization layer on its own, see the
> [Authorization](../Authorization/README.md) sample.

## What it shows

Two operator accounts protecting one in-process Orleans silo that exposes the
read-only State API over gRPC:

- **admin** - a bootstrap administrator. It seeds the trees, authors rules, and
  can read everything.
- **reader** - an ordinary user granted **read-only** access to a single tree
  (`orders`) and nothing else.

Each account is a salted **PBKDF2-SHA256** password hash (never the plaintext),
published in the `LATTICE_STATE_USER_<name>` environment variable the authorizer
looks up by username. In a real deployment an operator mints these with the
`tools/` helper scripts and sets the variables out-of-band; the sample mints
them in-process so it runs with a single command.

Two layers cooperate on every call:

1. **Authentication (transport).** `EnvVarCredentialAuthorizer` validates the
   inbound `authorization: Basic base64(user:pass)` header against the stored
   hash. A wrong password or a missing credential is rejected before any tree is
   touched. This is an authentication front door only - it does not consult the
   target tree or operation.
2. **Authorization (data plane).** A small `ILatticeStateApiCredentialBridge`
   lifts the authenticated username into the ambient caller credential, an
   `ILatticeCredentialAuthenticator` maps it to a subject id, and the
   default-deny `AddLatticeAuth` gate enforces the per-tree rule. The reader can
   read `orders`; the `ledger` tree is hidden from it entirely.

The demo runs in four acts:

1. **Create accounts.** Mint the two salted password hashes and publish them as
   environment variables.
2. **Stand up the silo.** Start one silo plus the State API gRPC surface with
   authentication required, then seed `orders` (3 entries) and `ledger` (2
   entries) as the bootstrap administrator.
3. **Authenticate.** Present each account's Basic credential over gRPC. The
   correct passwords authenticate; a wrong password and an anonymous call are
   both rejected with `PermissionDenied`.
4. **Authorize per-tree.** Scan both trees as each account. The admin sees
   everything; the reader sees `orders` but the `ledger` tree reads as empty
   because the gate hides it rather than disclosing its existence.

## Run it

```
dotnet run --project samples/PasswordProtection
```

## Expected output

```
== Act 1: create two operator accounts ==
  admin  -> env LATTICE_STATE_USER_admin   (salted pbkdf2-sha256)  role: bootstrap administrator
  reader -> env LATTICE_STATE_USER_reader  (salted pbkdf2-sha256)  role: read-only on tree 'orders'

== Act 2: start single silo + State API gRPC (auth required) ==
  Silo + state-API gRPC listening on http://localhost:5223
  Seeded tree 'orders' with 3 entries; tree 'ledger' with 2 entries.

== Act 3: authenticate over gRPC (username/password) ==
  admin  correct password -> authenticated
  reader correct password -> authenticated
  reader WRONG   password -> rejected (PermissionDenied)
  no credentials          -> rejected (PermissionDenied)

== Act 4: authorize per-tree reads (tied to the authenticated user) ==
  admin  scan 'orders' -> 3 entries ; scan 'ledger' -> 2 entries   (bootstrap admin: sees all)
  reader scan 'orders' -> 3 entries ; scan 'ledger' -> 0 entries   (granted 'orders' read; 'ledger' hidden)

[OK] username/password authenticated both users; per-tree rules limited 'reader' to 'orders'.
```

## When to use

- Single-cluster deployments that expose the State API gRPC surface to operators
  or tools and want a simple username/password front door without wiring up an
  external identity provider.
- Deployments that need the password front door **and** per-tree authorization,
  so a given account can read only the trees it was granted.

## When not to use

- Deployments that already authenticate with an OIDC/JWT identity provider - use
  a bearer-token bridge instead of the Basic credential authorizer. The
  authorization layer underneath is identical.
- Any surface exposed without TLS. The `Basic` credential is only as safe as the
  channel it rides: **terminate TLS** at the channel or an outer boundary so the
  credential is never sent in clear text. This sample uses plaintext h2c purely
  to stay dependency-free on `localhost`.

## Not production-hardened: do not expose this sample publicly

This sample is a **localhost teaching artifact**. Even with the credentials
changed and a TLS listener bolted on, the project as written is **not** safe to
expose to the public internet. Adding TLS alone does not close the gaps below;
each needs a deliberate change before this shape goes anywhere untrusted.

- **It runs plaintext h2c, not TLS.** The host sets the process-global
  `System.Net.Http.SocketsHttpHandler.Http2UnencryptedSupport` switch and binds
  Kestrel to `localhost` with `HttpProtocols.Http2` (no certificate). To serve
  real TLS you must remove that switch, bind a public interface, and call
  `UseHttps(...)` with a real certificate. Leaving the unencrypted switch set
  keeps a cleartext-downgrade path open even after you add an HTTPS listener, and
  port-forwarding the sample as-is sends the `Basic` credential in clear text.
- **The passwords are compiled into the binary.** The demo declares the
  passwords as constants and mints the hashes in-process at startup for a
  one-command run. That means "change the password" would mean editing source and
  shipping a recoverable secret in the assembly. A real deployment must set the
  `LATTICE_STATE_USER_*` variables out-of-band (for example with the `tools/`
  helper scripts) and delete the in-process minting entirely. Generate each hash
  once with `tools/new-lattice-state-credential.sh` (or
  `tools/New-LatticeStateCredential.ps1`) - it prints only the salted hash, never
  the plaintext - and inject it as the `LATTICE_STATE_USER_<username>` variable.
  When deploying with Docker, pass that variable through your orchestrator's
  secret mechanism rather than baking it into the image: a Docker/Swarm or
  Kubernetes secret surfaced as an env var, or `docker run --env-file` pointing
  at a file kept out of source control. Avoid `-e LATTICE_STATE_USER_...=...` on
  the command line (it lands in shell history and `docker inspect`) and never
  `ENV`/`ARG` the hash in the `Dockerfile` (it is baked into an image layer). The
  value you inject is the hash, so a leak still forces an attacker through
  PBKDF2, but treat it as a secret regardless.
- **Unauthenticated requests can exhaust CPU.** To keep the authorizer free of a
  user-existence timing oracle, **every** attempt - including an unknown username
  and a locked-out account - spends a full, deliberately expensive PBKDF2
  verification. That safety property is also an amplifier: an anonymous flood
  forces expensive hashing per request and can saturate CPU. A public deployment
  needs an upstream rate limiter / WAF / connection cap in front; the sample has
  none.
- **A known username can be locked out on purpose.** The failed-attempt lockout
  is per-username, and once locked the account is refused until the window
  expires - even with the correct password. Anyone who knows or guesses a
  username (`admin` is an obvious target) can keep the real operator locked out
  with wrong guesses. Public exposure wants IP-scoped throttling in front of the
  per-username lockout, not the lockout alone.
- **Failed-auth telemetry is silenced.** The sample clears all logging providers
  for a clean console, which drops the authorizer's own failed-auth warnings. A
  public surface should capture those events for detection and alerting.
- **The bootstrap administrator bypasses the data-plane gate entirely.** That is
  by design, but it makes the admin credential the crown jewels: its compromise
  grants full read access to every tree. Keep the bootstrap set as small as
  possible and treat that secret accordingly.


## Notes on this sample

- The password hash iteration count is deliberately **not** printed. It is an
  implementation detail of `LatticePasswordHash` that tracks current guidance and
  changes over time; hard-coding it in sample output would go stale.
- The bootstrap administrator seeds the trees and authors the reader's rule
  before any rule exists. Production should keep the bootstrap set as small as
  possible and grant everything else through rules.
- The authorization gate reads a compiled policy snapshot that rebuilds off the
  policy-tree change feed, so the sample polls briefly after authoring the rule
  before exercising enforcement.

## Feature docs

- [docs/lattice.api.state/security.md](../../docs/lattice.api.state/security.md)
- [docs/lattice.api.state.grpc/README.md](../../docs/lattice.api.state.grpc/README.md)
- [docs/lattice.auth/README.md](../../docs/lattice.auth/README.md)
- [docs/lattice.membership/README.md](../../docs/lattice.membership/README.md)

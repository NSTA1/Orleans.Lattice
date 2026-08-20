# Transport Security

`Orleans.Lattice.Replication` ships a secure-by-default shared-secret authenticator. The receiver rejects every Lattice replication call that does not carry a valid secret, and the sender refuses to connect to non-`https://` peers unless the host opts in explicitly. The surface is transport-agnostic: the secret-source seam lives in the core replication package, so the same primitives can be wired into a future non-gRPC transport without change.

## Threat model and posture

The authenticator addresses three concrete risks:

1. **Unauthenticated inbound replication.** A network attacker that can reach the receiver's gRPC endpoint can otherwise inject `WalRecord`s into the local cluster's apply pipeline.
2. **Misconfigured plaintext shipping.** A host accidentally pointing a sender at an `http://` peer would leak both the wire payload and the shared-secret header in cleartext.
3. **Secrets committed to source control.** Hosts that bind secrets from `appsettings.json` (or any file-backed configuration provider rooted under the application directory) routinely commit the file to source control or bake it into a container image.

The package fails closed on all three by default. Custom secret sources, plaintext opt-out, and the hostile-config scan toggle are explicit, named opt-ins.

## Default surface: environment variables

`AddLatticeReplication(...)` registers an environment-variable-backed secret source as the default `ILatticeReplicationSecretSource`. It reads the following variables (all prefixed `LATTICE_REPLICATION_`):

| Variable | Purpose |
|---|---|
| `LATTICE_REPLICATION_SECRET` | Cluster-wide outbound shared secret. Stamped on every batch the local cluster ships, except where a per-peer override is set. |
| `LATTICE_REPLICATION_ACCEPTED_SECRETS` | Comma- or semicolon-separated list of secrets accepted on inbound batches. Operators publish the next-generation secret here alongside the current one before flipping `LATTICE_REPLICATION_SECRET` on every silo, so the rotation is zero-downtime. |
| `LATTICE_REPLICATION_PEER_SECRET__<CLUSTERID>` | Per-peer outbound override. The cluster id is upper-snake-cased; the double-underscore separator avoids ambiguity when the id itself contains an underscore (e.g. `LATTICE_REPLICATION_PEER_SECRET__US_WEST_2` for `cluster=us-west-2`). |
| `LATTICE_REPLICATION_ALLOW_SOURCE_TREE_SECRETS` | Escape hatch that disables the startup hostile-config scan. Set to `1`, `true`, `yes`, or `on` to opt out. See [Hostile-config scan](#hostile-config-scan) below. |

Secret material itself is **not** an option. `LatticeReplicationSecurityOptions` exposes only policy (authenticator on/off, refresh interval, scan toggle); the secret strings flow through the `ILatticeReplicationSecretSource` seam.

Use `LatticeReplicationSharedSecret.Generate()` (32-byte URL-safe base64 by default) to produce values that pass the well-formedness check.

## Custom secret sources

For hosts that pull secrets from Azure Key Vault, AWS Secrets Manager, HashiCorp Vault, or any other store, implement `ILatticeReplicationSecretSource` and replace the default registration. The example below uses a self-contained stub; in a real host the source would call its underlying store.

```csharp verify
public sealed class MyVaultSecretSource : ILatticeReplicationSecretSource
{
    public ValueTask<string?> GetOutboundSecretAsync(string peerClusterId, CancellationToken ct)
        => new("secret-loaded-from-vault");

    public ValueTask<LatticeReplicationAcceptedSecrets> GetAcceptedSecretsAsync(CancellationToken ct)
        => new(LatticeReplicationAcceptedSecrets.Empty);
}
```

Then register the source via the typed overload. The implementation is activated through DI as a singleton, so it can declare constructor dependencies on any other registered service:

```csharp
siloBuilder.AddLatticeReplicationSecrets<MyVaultSecretSource>();
```

The factory overload is the right tool when the custom source wraps a pre-existing configured client:

```csharp
siloBuilder.AddLatticeReplicationSecrets(sp => new MyVaultSecretSource());
```

For non-file configuration providers (Azure App Configuration, Kubernetes secrets surfaced as environment variables, etc.), bind directly from a configuration section:

```csharp verify
var configuration = new Microsoft.Extensions.Configuration.ConfigurationBuilder().Build();
siloBuilder.AddLatticeReplicationSecretsFromConfiguration(
    configuration.GetSection("LatticeReplication:Secrets"));
```

The configuration section binds to a record with `Secret`, `AcceptedSecrets`, and `PeerSecrets` keys. The hostile-config scan still runs, so a configuration section that bottoms out in `appsettings.json` is still rejected at startup.

## Hostile-config scan

A hosted startup validator runs as an `IHostedService` and inspects every registered `IConfigurationProvider` at startup. If it finds a key whose name matches the secret-shaped pattern (`*Secret*`, `*Password*`, `*Token*`) **and** that key resolves through a file-backed provider rooted under the application directory, startup fails with a diagnostic that names the offending key and file path.

The check exists because the most common path to a leaked secret is a developer pasting it into `appsettings.json` (or its `Development` / `Production` variants), committing the file to source control, and discovering the leak weeks later. The scan does not inspect values; only the (key name, provider type, file path) tuple, so it cannot itself surface a secret.

To opt out, set `LATTICE_REPLICATION_ALLOW_SOURCE_TREE_SECRETS=1`. The toggle is environment-variable-only and intentionally not a `LatticeReplicationSecurityOptions` flag - making it a code option would route through `IConfiguration` and create a circular hostile-config path.

Tests and minimal hosts that do not register an `IConfiguration` root are unaffected; the validator resolves `IConfiguration` optionally and is a no-op when no configuration is present.

## Security policy options

`LatticeReplicationSecurityOptions` carries the non-secret policy knobs:

| Member | Default | Semantics |
|---|---|---|
| `RequireAuthentication` | `true` | When `true`, the gRPC server interceptor rejects unauthenticated calls. Setting to `false` is appropriate only for in-process loopback test fixtures that do not provision a secret. |
| `SecretRefreshInterval` | `30 s` | How often the caching secret provider re-queries the underlying source. Lower values reduce rotation latency at the cost of more calls into the secret store. |
| `ScanConfigurationForSecrets` | `true` | Disables the hostile-config scan when set to `false`. The environment-variable escape hatch is preferred for opt-out because it is auditable in deployment manifests. |

Configure via the standard options pattern:

```csharp verify
siloBuilder.ConfigureLatticeReplicationSecurity(o =>
{
    o.SecretRefreshInterval = TimeSpan.FromMinutes(5);
});
```

## gRPC transport behavior

The gRPC package (`Orleans.Lattice.Replication.Grpc`) layers transport mechanics on top of the core secret-source seam. The sender:

- **Refuses non-`https://` endpoints** unless `LatticeReplicationGrpcOptions.AllowPlaintextEndpoints` is explicitly set. The check runs at channel-resolution time, so a misconfigured `Peers` entry fails fast on the first batch dispatched to that peer rather than silently downgrading. When the opt-out is set and an insecure channel is actually built, the sender logs a warning and increments the `orleans.lattice.replication.grpc.insecure_channel` counter (tagged with the peer cluster id and the transport name) so that an accidental production plaintext downgrade is observable rather than silent.
- **Attaches the outbound secret as gRPC `CallCredentials`** whenever the secret source returns a non-empty value. The credentials are added to the channel options the package builds, then `ConfigureChannel(...)` runs - so a host that needs to replace the credentials chain entirely (e.g. mTLS-only with no shared secret) can do so unconditionally.
- **Stamps the local cluster id** as the `x-lattice-replication-origin` header on every call, sourced from `LatticeReplicationGrpcOptions.LocalClusterId` or, if unset, from `LatticeReplicationOptions.ClusterId`.

The receiver-side auth interceptor is registered globally on the gRPC service, scoped by service-name prefix so co-hosted gRPC services in the same ASP.NET Core app are unaffected. It rejects:

- **Calls without the `x-lattice-replication-secret` header** with `StatusCode.Unauthenticated`.
- **Calls whose secret is not in the accepted-set snapshot** with `StatusCode.PermissionDenied`.

The accepted-set check uses `LatticeReplicationSharedSecret.FixedTimeEquals` to keep comparison time independent of how close the candidate secret is to a real one.

## Headers on the wire

| Header | Direction | Purpose |
|---|---|---|
| `x-lattice-replication-secret` | sender to receiver | Authenticator material. Compared against the accepted-set snapshot in constant time. |
| `x-lattice-replication-origin` | sender to receiver | Local cluster id, used for diagnostic logging and metric tagging. Not authoritative for the apply path; the canonical origin id lives inside the envelope. |

The legacy sample header `X-Replication-Token` is retired. Hosts that depended on it should migrate to `x-lattice-replication-secret` via the env-var or custom secret source paths above.

## mTLS

mTLS is not a substitute for the shared-secret authenticator and not required by default, but the two compose. Wire the mTLS credentials through `LatticeReplicationGrpcOptions.ConfigureChannel` and the receiver's ASP.NET Core authentication middleware; the shared-secret interceptor runs in addition to whatever authentication policy the host configures.

A host that needs mTLS-only and no shared secret can do so by registering a custom `ILatticeReplicationSecretSource` that returns empty for every peer and setting `LatticeReplicationSecurityOptions.RequireAuthentication = false`. The configuration safety validator still runs; the receiver's authentication is now whatever the ASP.NET Core middleware enforces.

## Rotation

Zero-downtime rotation uses the accepted-set:

1. Generate the new secret on one silo and publish it as `LATTICE_REPLICATION_ACCEPTED_SECRETS=<old>,<new>` on every silo. The receiver now accepts both the old and the new secret.
2. Wait for `SecretRefreshInterval` plus a small margin on every silo so the new accepted-set propagates through the caching secret provider.
3. Flip `LATTICE_REPLICATION_SECRET=<new>` on every silo. Outbound batches now ship the new secret.
4. Remove the old secret from `LATTICE_REPLICATION_ACCEPTED_SECRETS`. Receivers now reject the old secret.

Custom secret sources implement the same protocol: the `GetAcceptedSecretsAsync` snapshot carries every secret accepted right now, including the next-generation one during the rotation window.

## Caveats

- **The hostile-config scan inspects key names, not values.** A secret stored under a non-secret-shaped key name (e.g. `Setting42`) is not flagged. The right answer is to route secret material through `ILatticeReplicationSecretSource`, not to disguise it in configuration.
- **`AllowPlaintextEndpoints` is a per-transport opt-out.** Setting it on the gRPC sender does not affect any other transport that may be registered alongside.
- **`RequireAuthentication = false` is the only loopback escape hatch on the receiver.** It disables the interceptor entirely for that host; do not use it in production.

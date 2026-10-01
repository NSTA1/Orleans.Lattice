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
| `LATTICE_REPLICATION_ACCEPTED_SECRETS` | Comma- or semicolon-separated list of secrets accepted on inbound batches, in addition to the local `LATTICE_REPLICATION_SECRET`, which is always accepted. Operators publish the next-generation secret here alongside the current one before flipping `LATTICE_REPLICATION_SECRET` on every silo. With origin binding on (the default) calls between silos that disagree are still refused during the flip - see [Rotation](#rotation). |
| `LATTICE_REPLICATION_PEER_SECRET__<CLUSTERID>` | Per-peer outbound override. The cluster id is upper-snake-cased; the double-underscore separator avoids ambiguity when the id itself contains an underscore (e.g. `LATTICE_REPLICATION_PEER_SECRET__US_WEST_2` for `cluster=us-west-2`). |
| `LATTICE_REPLICATION_ALLOW_SOURCE_TREE_SECRETS` | Escape hatch that disables the startup hostile-config scan. Set to `1`, `true`, or `yes` (case-insensitive) to opt out. See [Hostile-config scan](#hostile-config-scan) below. |

Secret material itself is **not** an option. `LatticeReplicationSecurityOptions` exposes only policy (authenticator on/off, origin binding, refresh interval, scan toggle); the secret strings flow through the `ILatticeReplicationSecretSource` seam.

Use `LatticeReplicationSharedSecret.Generate()` (32 random bytes, rendered as 43 URL-safe base64 characters, by default) to produce values that pass `LatticeReplicationSharedSecret.IsWellFormed` (at least `MinimumLength`, 32, characters). Nothing enforces that check at runtime; it is a helper for hosts that want to validate a secret themselves.

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

```csharp verify
public sealed class MyVaultSecretSource : ILatticeReplicationSecretSource
{
    public ValueTask<string?> GetOutboundSecretAsync(string peerClusterId, CancellationToken cancellationToken)
        => new("secret-loaded-from-vault");

    public ValueTask<LatticeReplicationAcceptedSecrets> GetAcceptedSecretsAsync(CancellationToken cancellationToken)
        => new(LatticeReplicationAcceptedSecrets.Empty);
}

public static void Register(ISiloBuilder siloBuilder)
{
    siloBuilder.AddLatticeReplicationSecrets<MyVaultSecretSource>();
}
```

The factory overload is the right tool when the custom source wraps a pre-existing configured client:

```csharp verify
public sealed class MyVaultSecretSource : ILatticeReplicationSecretSource
{
    public ValueTask<string?> GetOutboundSecretAsync(string peerClusterId, CancellationToken cancellationToken)
        => new("secret-loaded-from-vault");

    public ValueTask<LatticeReplicationAcceptedSecrets> GetAcceptedSecretsAsync(CancellationToken cancellationToken)
        => new(LatticeReplicationAcceptedSecrets.Empty);
}

public static void Register(ISiloBuilder siloBuilder)
{
    siloBuilder.AddLatticeReplicationSecrets(_ => new MyVaultSecretSource());
}
```

For non-file configuration providers (Azure App Configuration, Kubernetes secrets surfaced as environment variables, etc.), bind directly from a configuration section:

```csharp verify
var configuration = new Microsoft.Extensions.Configuration.ConfigurationBuilder().Build();
siloBuilder.AddLatticeReplicationSecretsFromConfiguration(
    configuration.GetSection("LatticeReplication:Secrets"));
```

The source reads the section's `Secret`, `AcceptedSecrets`, and `PeerSecrets` keys; a `PeerSecrets` entry for the target cluster id overrides `Secret` on outbound calls. The hostile-config scan still runs, so a section at the conventional `LatticeReplication:Secrets` path that bottoms out in `appsettings.json` is still rejected at startup.

## Hostile-config scan

A hosted startup validator runs as an `IHostedService` and inspects every registered `IConfigurationProvider` at startup. It checks the replication secret keys specifically - the flat `LatticeReplication:Secret` and `LatticeReplication:AcceptedSecrets` spellings and the nested `LatticeReplication:Secrets:Secret`, `LatticeReplication:Secrets:AcceptedSecrets`, and `LatticeReplication:Secrets:PeerSecrets:*` shape - and if a populated one resolves through a file-backed provider whose file lies under the application directory, startup fails with a diagnostic that names the offending key and file path. The .NET user-secrets store lives outside the application directory and does not trip it.

The check exists because the most common path to a leaked secret is a developer pasting it into `appsettings.json` (or its `Development` / `Production` variants), committing the file to source control, and discovering the leak weeks later. The scan does not inspect values; only the (key name, provider type, file path) tuple, so it cannot itself surface a secret.

To opt out, set `LATTICE_REPLICATION_ALLOW_SOURCE_TREE_SECRETS` to `1`, `true`, or `yes`, or set `LatticeReplicationSecurityOptions.ScanConfigurationForSecrets` to `false`; either one skips the scan and logs a warning at startup. The environment variable is the preferred escape hatch: the validator reads it straight from the process environment rather than through `IConfiguration`, and it is auditable in deployment manifests.

Tests and minimal hosts that do not register an `IConfiguration` root are unaffected; the validator resolves `IConfiguration` optionally and is a no-op when no configuration is present.

## Security policy options

`LatticeReplicationSecurityOptions` carries the non-secret policy knobs:

| Member | Default | Semantics |
|---|---|---|
| `RequireAuthentication` | `true` | When `true`, the gRPC server interceptor rejects unauthenticated calls. Setting to `false` is appropriate only for in-process loopback test fixtures that do not provision a secret. |
| `BindCredentialToOriginCluster` | `true` | When `true`, an authenticated call must also present the secret this cluster would itself send to the origin cluster the call claims. It has no effect while `RequireAuthentication` is `false`. See [`BindCredentialToOriginCluster`](configuration.md#transport-security---latticereplicationsecurityoptions) for how that secret is resolved and which secret schemes must turn it off. |
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
- **Attaches the outbound secret as gRPC `CallCredentials`** whenever the secret source returns a non-empty value. The credentials are added to the channel options the package builds, then `ConfigureChannel(...)` runs - so a host can replace the credentials chain entirely, although the origin header described next rides the same credentials (see [mTLS](#mtls) for a shared-secret-free setup that keeps it).
- **Stamps the local cluster id** as the `x-lattice-replication-origin` header on every call, sourced from `LatticeReplicationGrpcOptions.LocalClusterId` or, if unset, from the cluster-wide `LatticeReplicationOptions.ClusterId` - the unnamed options instance that `AddLatticeReplication` and `ConfigureLatticeReplication` without a tree name configure, never a per-tree override. The value is fixed when a peer's channel is built, so every tree that talks to that peer sends the same header. The header rides the package's call credentials, so a `ConfigureChannel` that replaces them stops it being sent, and with the default security options the receiver then refuses every call (see below). The replication package fills the origin field of every push, peer high-water-mark probe, and content-manifest exchange request with the sending tree's own `ClusterId`, resolved per tree, and the receiver refuses those calls when the header is missing or that field differs from it (see below). A tree's calls are therefore refused whenever its `ClusterId` differs from the header value - for example a `LocalClusterId` that differs from `ClusterId`, or a per-tree `ClusterId` override that differs from the cluster-wide value. Leave `LocalClusterId` unset, or set it to the same value as `ClusterId`, and give no replicated tree a per-tree `ClusterId` that differs from the cluster-wide value.

The receiver-side auth interceptor is registered globally on the gRPC service, scoped by service-name prefix so co-hosted gRPC services in the same ASP.NET Core app are unaffected. It rejects:

- **Calls without the `x-lattice-replication-secret` header** with `StatusCode.Unauthenticated`.
- **Calls whose secret is not in the accepted-set snapshot** with `StatusCode.PermissionDenied`.
- **Calls whose secret is not the one this cluster would itself send to the origin they claim** with `StatusCode.PermissionDenied`, while `LatticeReplicationSecurityOptions.BindCredentialToOriginCluster` is `true` (the default). A call with no `x-lattice-replication-origin` header, an origin for which no secret resolves, and a mismatched secret all get the same refusal. See [`BindCredentialToOriginCluster`](configuration.md#transport-security---latticereplicationsecurityoptions) for how the secret is resolved and what the binding establishes under cluster-wide and per-peer secrets.

Beyond the shared secret, the peer-read RPCs (digest probe, Merkle walk, peer high-water mark, and content-manifest exchange) re-resolve the peer-supplied tree name against the receiving cluster's own replication enrollment and refuse, with `StatusCode.PermissionDenied`, any tree that is not enrolled there - so a peer holding the mesh secret cannot aim those system-origin reads at a tree the cluster keeps local. The snapshot metadata and snapshot stream RPCs apply the same check on the exporting side before any tree is read, and refuse a non-enrolled tree with `StatusCode.PermissionDenied` too. The push, peer high-water-mark, and content-manifest RPCs also compare the origin cluster id the request declares (for a push, the batch envelope's) with the `x-lattice-replication-origin` header, and refuse with `StatusCode.PermissionDenied` a request that carries no header or whose declared origin differs from it. The saga control RPCs refuse a call that carries no header, or whose declared coordinator cluster id differs from it, and then pass the header's cluster id to an `ISagaPeerAuthorizer` gate, which by default admits only cluster ids present in `LatticeReplicationGrpcOptions.Peers`. These per-RPC checks run in the services themselves, so they apply even when `RequireAuthentication` is `false`.

The accepted-set check uses `LatticeReplicationSharedSecret.FixedTimeEquals` to keep comparison time independent of how close the candidate secret is to a real one.

## Headers on the wire

| Header | Direction | Purpose |
|---|---|---|
| `x-lattice-replication-secret` | sender to receiver | Authenticator material. Compared against the accepted-set snapshot in constant time and, with origin binding on, against the secret this cluster sends to the claimed origin. |
| `x-lattice-replication-origin` | sender to receiver | The sender's local cluster id. With origin binding on (the default) the interceptor requires it and binds it to the presented secret. The push, peer high-water-mark, content-manifest, and saga control RPCs refuse a call without it even when `RequireAuthentication` is `false`; the first three also refuse a request whose declared origin differs from it, and the saga control service refuses a differing coordinator id and authorizes the caller by it. |

The legacy sample header `X-Replication-Token` is retired. Hosts that depended on it should migrate to `x-lattice-replication-secret` via the env-var or custom secret source paths above.

## mTLS

mTLS is not a substitute for the shared-secret authenticator and not required by default, but the two compose. Wire the mTLS credentials through `LatticeReplicationGrpcOptions.ConfigureChannel` and the receiver's ASP.NET Core authentication middleware; the shared-secret interceptor runs in addition to whatever authentication policy the host configures.

A host that needs mTLS-only and no shared secret can do so by registering a custom `ILatticeReplicationSecretSource` that returns empty for every peer and setting `LatticeReplicationSecurityOptions.RequireAuthentication = false`. The configuration safety validator still runs; the receiver's authentication is now whatever the ASP.NET Core middleware enforces. Senders must still stamp the `x-lattice-replication-origin` header - the package's call credentials do so even when the secret source returns no secret - because the push, peer high-water-mark, content-manifest, and saga control RPCs refuse a call without it.

## Rotation

A receiver checks a presented secret against its accepted set and, with `BindCredentialToOriginCluster` on (the default), also against the single secret it would itself send to the caller's cluster - the per-peer override for that cluster, otherwise `LATTICE_REPLICATION_SECRET`. That second comparison ignores the accepted set (see [`BindCredentialToOriginCluster`](configuration.md#transport-security---latticereplicationsecurityoptions)), so with origin binding on a rotation refuses calls for as long as the two ends of a peering disagree. The procedure:

1. Generate the new secret and publish it as `LATTICE_REPLICATION_ACCEPTED_SECRETS=<old>,<new>` on every silo. The accepted set now holds both secrets.
2. Wait for `SecretRefreshInterval` plus a small margin on every silo so the new accepted set propagates through the caching secret provider.
3. Flip `LATTICE_REPLICATION_SECRET=<new>` on every silo of every cluster, as close together as possible. With origin binding on, a call between a silo that has flipped and one that has not is refused with `PermissionDenied`, in either direction, until both have flipped and their cached outbound secrets (each held for up to `SecretRefreshInterval`) have refreshed. The shipper backs off and retries a refused push without advancing its cursor, so shipping resumes once both ends agree; a snapshot bootstrap whose call is refused fails instead of retrying and has to be requested again. With origin binding off, the accepted set alone decides and this step refuses nothing.
4. Remove the old secret from `LATTICE_REPLICATION_ACCEPTED_SECRETS`. Receivers now reject the old secret.

A rotation that must refuse no call has to set `BindCredentialToOriginCluster` to `false` on every silo for its duration, giving up origin binding until it is restored.

Custom secret sources implement the same protocol: the `GetAcceptedSecretsAsync` snapshot carries every secret accepted right now, including the next-generation one during the rotation window, and with origin binding on, `GetOutboundSecretAsync` also supplies the secret each inbound call is compared against.

## Caveats

- **The hostile-config scan inspects a fixed set of key paths, not values.** Only the replication secret keys listed under [Hostile-config scan](#hostile-config-scan) are checked; a secret stored under any other key (e.g. `Setting42`, or a section bound from a non-conventional path) is not flagged. The right answer is to route secret material through `ILatticeReplicationSecretSource`, not to disguise it in configuration.
- **`AllowPlaintextEndpoints` is a per-transport opt-out.** Setting it on the gRPC sender does not affect any other transport that may be registered alongside.
- **`RequireAuthentication = false` is the only loopback escape hatch on the receiver.** It disables the interceptor entirely for that host - the shared-secret check and origin binding alike - but not the per-RPC origin-header checks; do not use it in production.

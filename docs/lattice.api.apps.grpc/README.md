# Orleans.Lattice.Api.Apps.Grpc

The code-first gRPC **binding** and public **client** for the
[`Orleans.Lattice.Api.Apps`](../lattice.api.apps/README.md) app control facade. It
exposes `ILatticeAppsControl` over a network transport as a thin adapter - the
control semantics and the `AppInstall` authorization live in the facade, and this
package only marshals them.

## What is it?

A code-first gRPC binding - `Grpc.AspNetCore` method definitions whose messages are
marshalled with the Orleans binary serializer, with no hand-written `.proto` - that
hosts the app control facade as a gRPC service and ships a strongly typed client for
calling it remotely. It mirrors the
[TreeAdmin gRPC binding](../lattice.api.treeadmin.grpc/README.md) packaging and
references only [`Orleans.Lattice.Api.Abstractions`](../lattice.api.abstractions/README.md).

There is **one** service for every app: each RPC carries the app slug it targets, so
installing a new app never adds a new service, credential bridge or interceptor.

## Core properties

- **Thin adapter.** Each RPC forwards one-to-one to an `ILatticeAppsControl` method;
  no control logic lives here.
- **Default-deny out of the box.** With `RequireAuthorization` left at its `true`
  default, the server interceptor consults the registered `ILatticeAppsApiAuthorizer`
  on every app RPC, and the registered default, `DenyAppsApiAuthorizer`, denies every
  call. A host must register its own `ILatticeAppsApiAuthorizer` (or turn
  `RequireAuthorization` off behind an outer authentication boundary) before any RPC
  other than `GetAuthScheme` is served. The package deliberately ships no allow-all
  authorizer. A refusal is returned as `PermissionDenied` before the call reaches the
  facade, which then applies its own `AppInstall` gate on top.
- **Server-classified operations.** The authorizer receives a
  `LatticeAppsApiAuthorizationContext` whose `Operation` (a `LatticeAppsApiOperation`)
  is derived from the bound RPC on the server, never from the payload, plus the
  caller-asserted app slug (or `null` for the listing and capability calls, which name
  no app). A call
  whose request shape does not match its bound RPC classifies as `Unknown` and is
  refused without consulting the authorizer; streaming calls to the service are
  refused outright.
- **Credential isolation per call.** The caller credential is read from a configurable
  header and bridged into the facade's authorization context for that call only. The
  header bridge is the default `ILatticeAppsApiCredentialBridge`; a host may register
  its own.
- **Discoverable auth.** A `GetAuthScheme` RPC advertises the accepted credential
  schemes so a client can self-configure. It is exempt from the authorizer so a client
  can learn how to sign in before it holds any credential, and it returns only the
  public scheme descriptors (`AuthSchemeDescriptor`) the host configured, through the
  replaceable `ILatticeAppsApiAuthSchemeSource`.
- **Sanitised failures.** Facade exceptions are mapped to gRPC status codes with a
  fixed, generic status message, so neither the exception message nor a stack trace or
  inner exception is forwarded: cancellation is `Cancelled`, an authorization or tenant
  denial `PermissionDenied`, an `ArgumentException` `InvalidArgument`, a
  `KeyNotFoundException` `NotFound`, an `InvalidOperationException`
  `FailedPrecondition`, and anything else `Internal`. A failed activation or a
  [tree ownership](../lattice.apps/README.md#tree-ownership) conflict on install, upgrade
  or enable is therefore `FailedPrecondition`; `Describe` returns each tree's
  `OwnershipConflict`, so a client can see which trees conflict before it installs.

## Service and RPCs

The gRPC service name is `orleans.lattice.api.apps`, so each method's full path is
`/orleans.lattice.api.apps/<Rpc>`.

| RPC | Facade method |
|---|---|
| `Install` | `InstallAsync` |
| `Enable` | `EnableAsync` |
| `Disable` | `DisableAsync` |
| `Uninstall` | `UninstallAsync` |
| `List` | `ListAsync` |
| `Describe` | `DescribeAsync` |
| `GetConsent` | `GetConsentAsync` |
| `UpdateConsent` | `UpdateConsentAsync` |
| `GetCapabilities` | `GetCapabilitiesAsync` |
| `UpdateRoleBindings` | `ILatticeAppRoleBindings.UpdateRoleBindingsAsync` |
| `GetAuthScheme` | Auth-scheme advertisement (unauthenticated). |

`UpdateRoleBindings` is served by the host's registered `ILatticeAppRoleBindings`,
or by an `ILatticeAppsControl` that also implements it; a host that serves neither
answers `Unimplemented`. Authorizers see it as
`LatticeAppsApiOperation.UpdateRoleBindings`. `LatticeAppsApiGrpcClient` implements
both `ILatticeAppsControl` and `ILatticeAppRoleBindings`.

### Catalogue, workspace and bridge services

The [catalogue, workspace and bridge](../lattice.api.apps/README.md#catalogue-workspace-and-bridge)
contracts are bound as three further code-first services. Each has its own name, its
own `Add*`/`Map*` pair, and a public client that implements the contract directly.
All three sit behind the same default-deny interceptor as the control service.

| Service | Registration | Client |
|---|---|---|
| `orleans.lattice.api.apps.catalog` | `AddLatticeAppCatalogApiGrpc` / `MapLatticeAppCatalogApiGrpc` | `LatticeAppCatalogApiGrpcClient` (`ILatticeAppCatalog`) |
| `orleans.lattice.api.apps.workspace` | `AddLatticeAppWorkspaceApiGrpc` / `MapLatticeAppWorkspaceApiGrpc` | `LatticeAppWorkspaceApiGrpcClient` (`ILatticeAppWorkspace`) |
| `orleans.lattice.api.apps.bridge` | `AddLatticeAppBridgeApiGrpc` / `MapLatticeAppBridgeApiGrpc` | `LatticeAppBridgeApiGrpcClient` (`ILatticeAppBridge`) |

Message sizes are bounded per method with a bounded marshaller rather than by
raising the channel's global message limit. The asset RPCs, for the icon and UI
bundle assets, accept a message of at most 2 MiB (the largest asset) plus 4 KiB of
envelope. Every bridge RPC is bounded too: a request at 64 KiB plus 8 KiB, and a
response at 1 MiB plus 64 KiB. A bridge failure crosses the wire as a status code
mapped one to one from its `AppBridgeFailure` (`Denied` to `PermissionDenied`,
`NotFound` to `NotFound`, `Invalid` to `InvalidArgument`, `TooLarge` to
`ResourceExhausted`, `Conflict` to `Aborted`, `Unavailable` to `Unavailable`),
carrying only the fixed message, and `LatticeAppBridgeApiGrpcClient` rethrows it as
an `AppBridgeException` with the same failure.

## Hosting the service

Register Orleans serialization, the facade, and the binding, then map the endpoint:

```csharp verify
using Orleans.Lattice.Api.Apps.Grpc;

public sealed class OperatorOnlyAppsApiAuthorizer : ILatticeAppsApiAuthorizer
{
    public Task<bool> IsAuthorizedAsync(
        LatticeAppsApiAuthorizationContext authorizationContext,
        CancellationToken cancellationToken)
        => Task.FromResult(
            authorizationContext.Call.RequestHeaders.GetValue("authorization") is { Length: > 0 });
}

public static class AppsGrpcHost
{
    public static void Configure(WebApplicationBuilder builder)
    {
        builder.Services.AddLatticeAppsApiGrpc(options =>
        {
            options.CredentialHeaderName = "authorization";
            options.CredentialScheme = "Bearer";
        });
        builder.Services.AddSingleton<ILatticeAppsApiAuthorizer, OperatorOnlyAppsApiAuthorizer>();
    }

    public static void Map(WebApplication app) => app.MapLatticeAppsApiGrpc();
}
```

The authorizer above only admits calls that present a credential, as an
illustration; a real policy decides per `Operation` and `AppSlug`. The facade's own
`AppInstall` gate still authorizes the credential on every call.

`AddLatticeAppsApiGrpc` registers the binding with the default-deny authorizer, the
header credential bridge and the auth interceptor; repeated registration keeps any
custom collaborators and does not duplicate the interceptor. The facade itself
(`ILatticeAppsControl`) is registered separately, normally with `AddLatticeAppsApi`
on the silo.

## Calling it remotely

```csharp verify
using Grpc.Core;
using Orleans.Lattice.Api.Apps.Grpc;

public static class AppsGrpcCaller
{
    public static async Task ListAsync(CallInvoker callInvoker, IServiceProvider serializerProvider)
    {
        var client = LatticeAppsApiGrpcClient.Create(callInvoker, serializerProvider);

        var schemes = await client.GetAuthSchemeAsync();
        var catalog = await client.ListAsync();
    }
}
```

`LatticeAppsApiGrpcClient` implements `ILatticeAppsControl`, so code written against
the facade runs unchanged against a remote cluster.

## Configuration reference

### `LatticeAppsApiGrpcOptions`

| Option | Type | Default | Meaning |
|---|---|---|---|
| `RequireAuthorization` | `bool` | `true` | Enforce the transport authorizer. Disable only behind an outer authentication boundary; the facade's `AppInstall` gate still applies. |
| `CredentialHeaderName` | `string` | `authorization` | The metadata key the caller credential is read from. It is bridged even when transport authorization is disabled. |
| `CredentialScheme` | `string` | `Bearer` | The optional prefix stripped from the credential and recorded as its authentication scheme. |
| `ActiveTenantHeaderName` | `string` | `lattice-active-tenant` (`LatticeActiveTenantAssertion.DefaultHeaderName`) | The asserted active-tenant header; null or empty disables it. The assertion is validated by the facade, never trusted by the binding. |
| `AdvertisedAuthSchemes` | `IList<AuthSchemeDescriptor> (get-only)` | empty | The public sign-in schemes returned by `GetAuthScheme`, in preference order. Never include credentials or user-specific data. |

## See also

- [Public API](api.md), [configuration](configuration.md), and [architecture](architecture.md)

- [App control facade](../lattice.api.apps/README.md)
- [Installable apps](../lattice.apps/README.md)
- [TreeAdmin gRPC binding](../lattice.api.treeadmin.grpc/README.md)

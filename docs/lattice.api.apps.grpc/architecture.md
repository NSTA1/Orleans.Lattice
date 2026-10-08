# Architecture

The binding exposes app facade contracts over code-first unary gRPC methods. Each facade group has a stable service; app slugs are request data, so new apps do not add endpoints. See the [binding guide](README.md) for the RPC matrix and detailed message/error behavior.

## Server authorization and credentials

Registration adds an interceptor and a default-deny transport authorizer. It classifies each call from the bound method and expected request shape; the payload cannot choose the authorization operation. Unknown method/request pairs are refused, and streaming calls to these app services are refused. `GetAuthScheme` is the unauthenticated public discovery exception. When authorization is enabled, the configured `ILatticeAppsApiAuthorizer` receives the server-classified operation, inbound call context, and asserted target slug when present. A host can replace the authorizer and credential bridge through dependency injection.

The credential bridge reads the configured metadata header and forwards the credential to the facade. The optional tenant assertion is read from its configured header, then validated by the facade's tenant resolution. Transport authorization does not replace in-process authorization. Control and administrative catalogue calls check `AppInstall`; workspace calls require a held app role, and bridge calls require an enabled installation, consent and an app-owned operation grant. Disabling the transport gate leaves these distinct facade checks active.

## Services and clients

Separate registration/mapping pairs expose control, catalogue, workspace, and bridge services. Their public clients implement the corresponding abstraction contracts. Asset and bridge payloads use per-method bounded marshalling rather than an increased global gRPC message limit.

Facade exceptions map to fixed status messages; exception details are not forwarded. Bridge failures preserve their closed failure category over the transport. See [configuration](configuration.md) for defaults and [public API](api.md) for the clients and contracts.

## See also

- [Public API](api.md)
- [Configuration](configuration.md)
- [Facade architecture](../lattice.api.apps/architecture.md)
- [gRPC binding guide](README.md)
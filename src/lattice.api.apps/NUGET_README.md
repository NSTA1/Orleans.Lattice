# Orleans.Lattice.Api.Apps

Transport-agnostic implementation of `ILatticeAppsControl`, the app lifecycle
and consent control surface. One generic facade dispatches by app slug; there
are no per-app services.

The facade owns no admin plane of its own. It composes the app registry
(install, upgrade, consent, list), the app source seam (describe a manifest
before installation, without loading app code) and the activation pipeline
(enable, disable, uninstall, activation status).

Register it on the silo after the core lattice and the apps add-on:

```text
siloBuilder
    .AddLattice(...)
    .AddLatticeApps()
    .AddLatticeAppsApi();
```

`AddLatticeAppsApi()` registers the facade as the `ILatticeAppsControl`
singleton that transport bindings such as `Orleans.Lattice.Api.Apps.Grpc` map,
and as the `ILatticeAppRoleBindings` singleton that replaces an installed app's
role-to-group bindings for its installed version. An enabled app is re-applied
afterwards, so a removed binding keeps no grant; a disabled app stays disabled.

Behaviour:

- Every verb except `GetCapabilitiesAsync` authorizes
  `LatticeOperation.AppInstall` over the cluster-wide scope through the shared
  access gate before it reads registry or source metadata. The capability probe
  is advisory and grants nothing.
- Operations run in the caller's active tenant (the default tenant when tenancy
  is off). Caller-supplied tree references in a ceiling are validated and
  tenant-composed at entry, then stored in their tenant-local form.
- Responses echo app slugs, app-local tree names and the pre-app ids of adopted
  trees, never a composed physical tree id. Exception messages are sanitized so no
  composed physical tree id crosses the facade.
- Uninstall soft-deletes an app's structural trees and never purges them itself;
  the core purges each soft-deleted tree once its soft-delete window elapses.

# Configuration

`Orleans.Lattice.Apps` has two option objects. `LatticeAppsOptions` controls startup reconciliation; `InImageAppSourceOptions` holds registrations. See the [manifest guide](README.md#validation) for format validation rules.

## Engine startup options

| Option | Type | Default | Validation and effect |
|---|---|---|---|
| `ReconcileOnStartup` | `bool` | `true` | Reconciles enabled installs in the background once per silo start. App failure is recorded and logged; host startup is not failed. |
| `StartupRetryDelay` | `TimeSpan` | 250 ms (`DefaultStartupRetryDelay`) | Must be positive. Initial delay before retrying the startup read; it doubles to the maximum. Waits are held to the timer maximum. |
| `StartupRetryMaxDelay` | `TimeSpan` | 30 s (`DefaultStartupRetryMaxDelay`) | Must be positive and at least `StartupRetryDelay`; caps the retry delay. |

Configure with `AddLatticeApps(options => ...)` on `ISiloBuilder` or `IServiceCollection`.

## In-image source options

| Option | Type | Default | Mutability and use |
|---|---|---|---|
| `Registrations` | `IList<InImageAppRegistration>` (get-only) | Empty list | Each entry specifies the slug, assembly, manifest resource name, publisher, and UI asset resource prefix. `AddLatticeApp` or `InImageAppSourceOptions.Register` appends entries. |

Registration makes an app available to install; it does not install, enable, or enroll the app. The source reads registrations when constructed. Publisher defaults to `first-party`. The default asset prefix is `{manifestResourceNamespace}.ui.` when stripping the manifest resource name's final two dot-separated segments leaves a non-empty prefix; otherwise it is `ui.`.

## Manifest bounds

These are input-format constraints, not mutable options. Exceeded limits return structured validation errors; adopted-id length is reported through adoption validation. The text parser measures UTF-16 characters while the stream parser measures UTF-8 bytes, so those two limits are not equivalent for non-ASCII input.

| Manifest field or section | Limit |
|---|---|
| JSON text / input stream | 1,048,576 UTF-16 characters / 1,048,576 UTF-8 bytes |
| Entries in a section or scopes of one role | 256 |
| Key, prefix, adopted id, schema family or provenance string | 1,024 characters |
| MCP tool description | 4,096 characters |
| Presentation display name | 60 characters |
| Presentation summary | 160 characters |
| Presentation description | 4,000 characters |
| Publisher display name | 80 characters |
| Presentation categories | 5 |
| Documentation URL | 2,048 characters; absolute HTTPS, no whitespace or user information |
| UI bundle assets (`AppUiBundle.MaxAssets`) | 256 |
| One UI asset (`AppUiBundle.MaxAssetBytes`) | 2 MiB |
| Whole UI bundle (`AppUiBundle.MaxBundleBytes`) | 16 MiB |
| UI bundle asset path (`AppUiBundle.MaxPathLength`) | 256 characters |

The [manifest validator](../../src/lattice.apps/Manifest/AppManifestValidator.Ui.cs), [format bounds](../../src/lattice.apps/Manifest/AppManifestLimits.cs) and [UI bundle](../../src/lattice.apps/Manifest/AppUiBundle.cs) define these constraints.

## See also

- [Public API](api.md)
- [Architecture](architecture.md)
- [Engine guide](README.md)
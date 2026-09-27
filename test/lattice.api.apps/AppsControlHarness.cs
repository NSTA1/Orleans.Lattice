using NSubstitute;
using Orleans.Lattice.Apps;
using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Api.Apps.Tests;

/// <summary>
/// Shared harness for the facade tests: substitutes for the registry, source and
/// pipeline, a recording access gate, a configurable tenant resolver, and builders
/// for registry records, manifests and activation outcomes.
/// </summary>
internal sealed class AppsControlHarness
{
    public const string Slug = "crm";
    public const string Version = "1.2.0";
    public const string OtherVersion = "2.0.0";

    public static readonly TenantId Acme = TenantId.Parse("acme");

    public IAppRegistry Registry { get; } = Substitute.For<IAppRegistry>();

    public IAppSource Source { get; } = Substitute.For<IAppSource>();

    public IAppActivationPipeline Pipeline { get; } = Substitute.For<IAppActivationPipeline>();

    public RecordingAccessGate Gate { get; } = new();

    public ConfigurableTenantResolver Tenants { get; } = new();

    public LatticeAppsControl Control => new(Registry, Source, Pipeline, Gate, Tenants);

    public TenantId Tenant => Tenants.Tenant;

    public static AppSlug AppSlugValue => AppSlug.Parse(Slug);

    public static AppVersion V(string version) => AppVersion.Parse(version);

    public static AppRegistryRecord Record(
        AppRegistryLifecycleState state,
        string version = Version,
        TenantId? tenant = null,
        AppCapabilityCeiling? ceiling = null,
        IReadOnlyList<AppRoleBinding>? bindings = null,
        string slug = Slug) =>
        new()
        {
            Isolation = new AppIsolationContext { Tenant = tenant ?? TenantId.Default, ClusterId = "test" },
            Slug = AppSlug.Parse(slug),
            Version = V(version),
            Provenance = new AppProvenance { Source = "in-image", Publisher = "first-party", Reference = "ref" },
            Ceiling = ceiling ?? AppCapabilityCeiling.Structural(LatticeOperation.Read | LatticeOperation.Write),
            CeilingVersion = V(version),
            RoleBindings = bindings ?? [AppRoleBinding.Create("reader", "g-readers")],
            State = state,
            Revision = 1,
        };

    public static AppManifest Manifest(string version = Version, string slug = Slug) =>
        new()
        {
            Identity = new AppIdentity { Slug = AppSlug.Parse(slug), Version = V(version) },
            Trees =
            [
                new AppTreeDeclaration { Name = "contacts", ShardCount = 2, Rebuildable = true },
                new AppTreeDeclaration { Name = "legacy", AdoptedTreeId = "legacy-contacts" },
            ],
            Roles =
            [
                new AppRoleDeclaration
                {
                    Name = "reader",
                    Operations = LatticeOperation.Read,
                    Scopes =
                    [
                        new AppScopeTemplate { Tree = "contacts" },
                        new AppScopeTemplate { Tree = "ledger", App = AppSlug.Parse("billing") },
                        new AppScopeTemplate { Tree = "contacts", App = AppSlug.Parse(slug), Kind = LatticeScopeKind.Prefix, KeyOrPrefix = "p/" },
                    ],
                },
                new AppRoleDeclaration { Name = "writer", Operations = LatticeOperation.Write, Scopes = [new AppScopeTemplate { Tree = "contacts" }] },
            ],
            Subscriptions =
            [
                new AppSubscriptionDeclaration { Name = "on-contact", Tree = "contacts" },
                new AppSubscriptionDeclaration { Name = "on-invoice", Tree = "ledger", App = AppSlug.Parse("billing"), KeyPrefix = "inv/" },
            ],
            McpTools = [new AppMcpToolDeclaration { Name = "find", Description = "Find contacts.", Role = "reader" }],
            Replication = [new AppReplicationDeclaration { Tree = "contacts", MergeMode = LatticeMergeMode.LwwRegister }],
            Schema = [new AppSchemaDeclaration { Tree = "contacts", Family = "contact", Version = 3, StrictIngest = true }],
        };

    public static AppActivationOutcome Outcome(
        AppActivationOperation operation,
        AppRegistryLifecycleState? state,
        AppActivationFailure failure = AppActivationFailure.None,
        bool changed = true,
        string? version = Version,
        string? diagnostic = null) =>
        new()
        {
            Tenant = TenantId.Default,
            Slug = AppSlugValue,
            Operation = operation,
            Failure = failure,
            Version = version is null ? null : V(version),
            State = state,
            Changed = changed,
            Diagnostics = diagnostic is null ? [] : [new AppManifestError("code", "$", diagnostic)],
        };

    public static AppActivationStatus Status(AppActivationOutcome last) =>
        new() { Tenant = TenantId.Default, Slug = AppSlugValue, LastOutcome = last };

    public static AppCapabilityCeilingDescriptor WireCeiling(params AppExceptionScope[] scopes) =>
        new() { AllowedOperations = LatticeOperation.Read, ApprovedExceptionScopes = [.. scopes] };

    public static AppInstallRequest InstallRequest(string version = Version, params AppExceptionScope[] scopes) =>
        new()
        {
            Slug = Slug,
            Version = version,
            RoleBindings = [new AppRoleBindingDescriptor { RoleName = "reader", GroupId = "g-readers" }],
            Ceiling = WireCeiling(scopes),
        };

    public void SourceResolves(AppManifest? manifest = null)
    {
        manifest ??= Manifest();
        var handle = Substitute.For<IAppActivationHandle>();
        handle.Identity.Returns(manifest.Identity);
        var provenance = new AppProvenance { Source = "in-image", Publisher = "contoso", Reference = "asm" };
        Source.ResolveAsync(Arg.Any<AppSlug>(), Arg.Any<AppVersion?>(), Arg.Any<CancellationToken>())
            .Returns(new ValueTask<AppSourceResult>(AppSourceResult.Resolved(manifest, provenance, handle)));
    }

    public void SourceReturns(AppSourceResult result) =>
        Source.ResolveAsync(Arg.Any<AppSlug>(), Arg.Any<AppVersion?>(), Arg.Any<CancellationToken>())
            .Returns(new ValueTask<AppSourceResult>(result));

    public void RegistryHas(AppRegistryRecord? record) =>
        Registry.GetAsync(Arg.Any<TenantId>(), Arg.Any<AppSlug>(), Arg.Any<CancellationToken>()).Returns(record);

    public void RegistryLists(params AppRegistryRecord[] records) =>
        Registry.ListForTenantAsync(Arg.Any<TenantId>(), Arg.Any<CancellationToken>()).Returns(_ => ToAsync(records));

    public void StatusIs(AppActivationStatus? status) =>
        Pipeline.GetStatusAsync(Arg.Any<TenantId>(), Arg.Any<AppSlug>(), Arg.Any<CancellationToken>()).Returns(status);

    public static AppRegistryTransitionResult Succeeded(AppRegistryRecord record, bool changed = true) =>
        new() { Record = record, Changed = changed };

    public static AppRegistryTransitionResult Rejected(AppRegistryTransitionError error, string? message = null) =>
        new() { Error = error, Message = message ?? "rejected" };

    /// <summary>Asserts that no registry, source or pipeline member was called.</summary>
    public void AssertEngineUntouched()
    {
        Assert.That(Registry.ReceivedCalls(), Is.Empty, "registry");
        Assert.That(Source.ReceivedCalls(), Is.Empty, "source");
        Assert.That(Pipeline.ReceivedCalls(), Is.Empty, "pipeline");
    }

    private static async IAsyncEnumerable<AppRegistryRecord> ToAsync(AppRegistryRecord[] records)
    {
        foreach (var record in records)
        {
            await Task.Yield();
            yield return record;
        }
    }
}

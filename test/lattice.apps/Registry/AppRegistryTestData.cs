using Microsoft.Extensions.Options;
using Orleans.Configuration;
using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Apps.Tests;

/// <summary>Shared builders for the app-registry unit tests.</summary>
internal static class AppRegistryTestData
{
    public const string ClusterId = "cluster-a";

    public static readonly DateTimeOffset Start = new(2026, 1, 2, 3, 4, 5, TimeSpan.Zero);

    public static readonly AppSlug Slug = AppSlug.Parse("notes");

    public static readonly AppVersion V1 = AppVersion.Parse("1.0.0");

    public static readonly AppVersion V2 = AppVersion.Parse("2.0.0");

    public static readonly TenantId Acme = TenantId.Parse("acme");

    public static AppRegistryInstallRequest Request(
        AppVersion? version = null,
        TenantId? tenant = null,
        AppSlug? slug = null,
        AppCapabilityCeiling? ceiling = null,
        IReadOnlyList<AppRoleBinding>? bindings = null) => new()
    {
        Tenant = tenant ?? TenantId.Default,
        Identity = new AppIdentity { Slug = slug ?? Slug, Version = version ?? V1 },
        Ceiling = ceiling ?? AppCapabilityCeiling.Structural(LatticeOperation.Read | LatticeOperation.Write),
        RoleBindings = bindings ?? new[] { AppRoleBinding.Create("reader", "readers") },
    };

    public static AppRegistry CreateRegistry(
        InMemoryAppRegistryStore store,
        ILatticeAccessGate? gate = null,
        ILatticeMembershipContext? membership = null,
        TimeProvider? time = null,
        IAppSource? source = null,
        AppTreeOwnershipLedger? ownership = null) =>
        new(
            store,
            new AppInstallAuthorizer(gate ?? RecordingAccessGate.AllowAll(), membership),
            Options.Create(new ClusterOptions { ClusterId = ClusterId }),
            ownership ?? CreateLedger(store),
            source ?? NullAppSource.Instance,
            time ?? new ManualTimeProvider(Start));

    public static AppTreeOwnershipLedger CreateLedger(
        InMemoryAppRegistryStore store,
        InMemoryAppTreeLedgerStore? ledger = null,
        FakeAppTreeFacts? facts = null,
        TimeProvider? time = null) =>
        new(ledger ?? new InMemoryAppTreeLedgerStore(), facts ?? new FakeAppTreeFacts(), store, time ?? new ManualTimeProvider(Start));

    public static AppRegistryRecord Record(
        AppRegistryLifecycleState state,
        TenantId? tenant = null,
        AppSlug? slug = null,
        AppVersion? version = null,
        AppVersion? ceilingVersion = null) => new()
    {
        Isolation = new AppIsolationContext { Tenant = tenant ?? TenantId.Default, ClusterId = ClusterId },
        Slug = slug ?? Slug,
        Version = version ?? V1,
        Provenance = new AppProvenance(),
        Ceiling = AppCapabilityCeiling.Structural(LatticeOperation.Read),
        CeilingVersion = ceilingVersion ?? version ?? V1,
        State = state,
        Revision = 1,
    };
}

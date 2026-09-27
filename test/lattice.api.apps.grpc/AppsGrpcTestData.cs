using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Api.Apps.Grpc.Tests;

internal static class AppsGrpcTestData
{
    public static AppProvenanceDescriptor Provenance { get; } = new()
        { Source = "in-image", Publisher = "test", Reference = "image:1" };

    public static AppCapabilityCeilingDescriptor Ceiling { get; } = new()
    {
        AllowedOperations = LatticeOperation.Read | LatticeOperation.Write,
        ApprovedExceptionScopes =
        [
            new() { Kind = LatticeScopeKind.Prefix, App = "other", Tree = "events", KeyOrPrefix = "visible/" },
            new() { Kind = LatticeScopeKind.Tree, AdoptedTreeId = "legacy" },
        ],
    };

    public static AppInstallRequest Install { get; } = new()
    {
        Slug = "demo", Version = "1.2.3", Ceiling = Ceiling,
        RoleBindings = [new() { RoleName = "reader", GroupId = "readers" }],
    };

    public static AppDescriptor Descriptor { get; } = new()
    {
        Slug = "demo", Version = "1.2.3", Provenance = Provenance, State = AppLifecycleState.Enabled,
        Ceiling = Ceiling, RoleBindings = Install.RoleBindings,
        Trees = [new() { Name = "events", Rebuildable = true, AdoptedTreeId = "legacy", ShardCount = 2,
            VirtualShardCount = 32, MaxLeafKeys = 100, MaxInternalChildren = 8, WalPartitions = 4,
            SoftDeleteDuration = TimeSpan.FromDays(2) }],
        Roles = [new() { Name = "reader", Operations = LatticeOperation.Read, Scopes =
            [new() { Tree = "events", App = "other", Kind = LatticeScopeKind.Prefix, KeyOrPrefix = "visible/" }] }],
        Subscriptions = [new() { Name = "changes", Tree = "events", App = "other", KeyPrefix = "visible/" }],
        McpTools = [new() { Name = "read", Description = "Reads events", Role = "reader" }],
        Replication = [new() { Tree = "events", MergeMode = LatticeMergeMode.OrSet }],
        Schema = [new() { Tree = "events", Family = "event", Version = 2, StrictIngest = true }],
    };

    public static AppConsentReport Consent { get; } = new()
        { Slug = "demo", Version = "1.2.3", Ceiling = Ceiling };

    public static AppLifecycleResult Lifecycle(AppLifecycleState state) => new()
        { Slug = "demo", Version = "1.2.3", State = state, Changed = true };
}

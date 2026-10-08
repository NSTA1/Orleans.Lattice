using System.Text;
using Orleans.Lattice.Api.Replication;
using Orleans.Lattice.Api.TenantAdmin;
using Orleans.Lattice.Apps;
using Orleans.Lattice.Apps.Sources;
using Orleans.Lattice.Auth;
using Orleans.Lattice.Membership;
using Orleans.Lattice.Replication;
using Orleans.Lattice.Samples.Explorer.TaskBoard;
using Orleans.Lattice.Tenancy;

namespace Orleans.Lattice.Samples.Explorer;

/// <summary>
/// Seeds each region with the sample's data, identities and policy. Every seed
/// is a fixed, small set: nothing here grows with the time the sample runs.
/// </summary>
/// <remarks>
/// Seeding runs as the trusted co-hosted process it is: data, membership and
/// policy writes run system-origin, which bypasses the deny-by-default gate
/// exactly as any co-hosted infrastructure component does. The tenant
/// lifecycle calls run as the platform operator, so they go through the same
/// fail-closed operator checks the Explorer's Tenancy area does.
/// </remarks>
internal static class SampleSeeder
{
    /// <summary>The id of the rule granting <c>operators</c> Read on the demo tree.</summary>
    public const string OperatorsReadRuleId = "operators-read-factory-floor";

    /// <summary>The id of the rule letting globex's admin read acme's orders, which acme offers globex through a grant.</summary>
    public const string SharedOrdersRuleId = "globex-admin-read-acme-orders";

    /// <summary>The tenant-local id of globex's own rule granting its <c>operators</c> group Read on its orders.</summary>
    public const string GlobexOrdersRuleId = "operators-read-orders";

    /// <summary>
    /// The tenant-local id of globex's own rule granting its <c>operators</c> group
    /// Read on its invoices, which <see cref="GlobexInvoicesPlatformRuleId"/> shadows.
    /// </summary>
    public const string GlobexInvoicesRuleId = "operators-read-invoices";

    /// <summary>The id of the Platform rule that denies alice globex's invoices, deciding before globex's own rules.</summary>
    public const string GlobexInvoicesPlatformRuleId = "platform-deny-alice-globex-invoices";

    /// <summary>How many invoices globex's <c>invoices</c> tree holds.</summary>
    public const int InvoicesPerTenant = 3;

    /// <summary>The tenant-scoped id of globex's <c>invoices</c> tree.</summary>
    public static string InvoicesTree { get; } = LatticeTenantTrees.Compose(TenantId.Parse(SampleIdentities.GlobexTenant), SampleIdentities.GlobexInvoicesTree);

    /// <summary>How many orders each tenant's <c>orders</c> tree holds.</summary>
    public const int OrdersPerTenant = 5;

    /// <summary>The task cards seeded into the acme tenant's task board.</summary>
    public static IReadOnlyList<(string Id, string Title, string Column)> AcmeTasks { get; } =
    [
        ("t-001", "Calibrate press 3", "todo"),
        ("t-002", "Order spare bearings", "doing"),
        ("t-003", "Review night-shift report", "done"),
    ];

    /// <summary>Seeds each region's sample identities and baseline policy.</summary>
    /// <param name="region">The region.</param>
    /// <param name="staticDirectory">Whether the static roster backs the directory, so the demo groups can be seeded.</param>
    /// <param name="log">Receives one line per seeded item.</param>
    /// <param name="cancellationToken">Cancels seeding.</param>
    public static async Task SeedRegionAsync(SampleRegion region, bool staticDirectory, Action<string> log, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(region);
        ArgumentNullException.ThrowIfNull(log);

        if (staticDirectory)
        {
            await SeedAccessAsync(region.Services, cancellationToken).ConfigureAwait(false);
            log($"[{region.Id}] Access: deny-by-default, 'operators' (member 'alice') may Read '{SampleIdentities.FactoryFloorTree}'.");
        }
    }

    /// <summary>
    /// Enrols the demo tree in replication from the primary region, then waits
    /// until the peer has learned the enrolment. Tenant trees are enrolled only
    /// after the tenant's first resident region is Online.
    /// </summary>
    /// <remarks>
    /// The enrolment is written once, in one region, and reaches the peer
    /// through the replicated runtime configuration. Enabling the same tree
    /// independently in both regions would leave the configuration's
    /// multi-value register holding two concurrent assignments, which every
    /// receiver treats as ambiguous and refuses. And a receiver that has not yet
    /// learned a tree's enrolment drops what it is sent, so nothing is written
    /// to the tree until the peer knows it.
    /// </remarks>
    /// <param name="primary">The region the enrolment is written in.</param>
    /// <param name="peer">The region that must learn it.</param>
    /// <param name="log">Receives one line per seeded item.</param>
    /// <param name="cancellationToken">Cancels enrolment.</param>
    public static async Task EnrolAsync(SampleRegion primary, SampleRegion peer, Action<string> log, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(primary);
        ArgumentNullException.ThrowIfNull(peer);
        ArgumentNullException.ThrowIfNull(log);

        foreach (var treeId in new[] { SampleIdentities.FactoryFloorTree })
        {
            using (LatticeSystemOrigin.Enter())
            {
                var control = primary.Services.GetRequiredService<ILatticeReplicationControl>();
                await control.EnableReplicationAsync(treeId, LatticeMergeMode.LwwRegister, cancellationToken: cancellationToken)
                    .ConfigureAwait(false);
            }

            if (!await WaitForEnrolmentAsync(peer, treeId, EnrolmentBudget, cancellationToken).ConfigureAwait(false))
            {
                throw new TimeoutException($"Region '{peer.Id}' has not learned replication enrolment for '{treeId}'.");
            }
        }

        log($"[{primary.Id}] Replication: '{SampleIdentities.FactoryFloorTree}' enrolled (last-writer-wins) with '{peer.Id}'.");
    }

    private static async Task EnrolTenantTreesAsync(
        SampleRegion primary, SampleRegion peer, CancellationToken cancellationToken)
    {
        foreach (var treeId in new[]
        {
            OrdersTree(SampleIdentities.AcmeTenant),
            OrdersTree(SampleIdentities.GlobexTenant),
            InvoicesTree,
        })
        {
            using (LatticeSystemOrigin.Enter())
            {
                var control = primary.Services.GetRequiredService<ILatticeReplicationControl>();
                await control.EnableReplicationAsync(
                    treeId, LatticeMergeMode.LwwRegister, cancellationToken: cancellationToken).ConfigureAwait(false);
            }

            if (!await WaitForEnrolmentAsync(peer, treeId, EnrolmentBudget, cancellationToken).ConfigureAwait(false))
            {
                throw new TimeoutException($"Region '{peer.Id}' has not learned replication enrolment for '{treeId}'.");
            }
        }
    }

    /// <summary>How long seeding waits for the peer to learn an enrolment before carrying on.</summary>
    public static TimeSpan EnrolmentBudget { get; } = TimeSpan.FromSeconds(20);

    /// <summary>
    /// Waits until <paramref name="region"/> resolves <paramref name="treeId"/> as
    /// enrolled with one unambiguous mode, or <paramref name="budget"/> elapses.
    /// </summary>
    /// <param name="region">The region.</param>
    /// <param name="treeId">The tree.</param>
    /// <param name="budget">The longest wait.</param>
    /// <param name="cancellationToken">Cancels the wait.</param>
    /// <returns>Whether the region learned the enrolment in time.</returns>
    public static async Task<bool> WaitForEnrolmentAsync(SampleRegion region, string treeId, TimeSpan budget, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(region);
        ArgumentException.ThrowIfNullOrEmpty(treeId);

        var control = region.Services.GetRequiredService<ILatticeReplicationControl>();
        using var budgetSource = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        budgetSource.CancelAfter(budget);
        using var poll = new PeriodicTimer(TimeSpan.FromMilliseconds(100));
        try
        {
            do
            {
                using (LatticeSystemOrigin.Enter())
                {
                    var report = await control.GetReplicationConfigAsync(budgetSource.Token).ConfigureAwait(false);
                    if (report.Trees.Any(tree => tree.Enabled && !tree.Ambiguous && string.Equals(tree.TreeId, treeId, StringComparison.Ordinal)))
                    {
                        return true;
                    }
                }
            }
            while (await poll.WaitForNextTickAsync(budgetSource.Token).ConfigureAwait(false));
        }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
        {
            // The budget elapsed.
        }

        return false;
    }

    /// <summary>
    /// Seeds what only the primary region holds: the demo tree's data and, on the
    /// estate, tenant definitions, tenant data and the task board installed in
    /// acme with a few cards. Replication carries them to the peer.
    /// </summary>
    /// <param name="region">The primary region.</param>
    /// <param name="peer">The peer region, or <see langword="null"/> for a single-region run.</param>
    /// <param name="log">Receives one line per seeded item.</param>
    /// <param name="cancellationToken">Cancels seeding.</param>
    public static async Task SeedPrimaryAsync(SampleRegion region, SampleRegion? peer, Action<string> log, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(region);
        ArgumentNullException.ThrowIfNull(log);

        if (peer is not null)
        {
            await SeedTenancyAsync(region, peer, cancellationToken).ConfigureAwait(false);
            log($"[{region.Id}] Tenancy: acme and globex seeded once; tenant definitions replicate to '{peer.Id}'. acme is Online in east and west; globex has no residency. Tenant orders, grants and delegated access are seeded in this region and replicate to the peer.");
        }

        var grains = region.Services.GetRequiredService<IGrainFactory>();
        using (LatticeSystemOrigin.Enter())
        {
            var tree = grains.GetGrain<ILattice>(SampleIdentities.FactoryFloorTree);
            for (var i = 0; i < SampleIdentities.MachineCount; i++)
            {
                await tree.SetAsync(MachineKey(i), Encoding.UTF8.GetBytes($"status-{i:D3}"), cancellationToken).ConfigureAwait(false);
            }
        }

        log($"[{region.Id}] Data: '{SampleIdentities.FactoryFloorTree}' seeded with {SampleIdentities.MachineCount} entries.");

        if (peer is null)
        {
            return;
        }

        // Activation enrols the app's declared tree in this region; the cards
        // are written once the peer has learned that enrolment too.
        var tasksTree = await InstallTaskBoardAsync(region.Services, TenantId.Parse(SampleIdentities.AcmeTenant), cancellationToken)
            .ConfigureAwait(false);
        await WaitForEnrolmentAsync(peer, tasksTree, EnrolmentBudget, cancellationToken).ConfigureAwait(false);
        using (LatticeSystemOrigin.Enter())
        {
            var tasks = grains.GetGrain<ILattice>(tasksTree);
            foreach (var (id, title, column) in AcmeTasks)
            {
                var card = $$"""{"title":"{{title}}","column":"{{column}}","created":"2026-01-01T09:00:00Z"}""";
                await tasks.SetAsync(TaskKey(id), Encoding.UTF8.GetBytes(card), cancellationToken).ConfigureAwait(false);
            }
        }

        log($"[{region.Id}] Apps: '{TaskBoardApp.Slug}' installed and enabled in tenant '{SampleIdentities.AcmeTenant}' ('editor' bound to '{SampleIdentities.AcmeEditorsGroup}'); its declared tree '{tasksTree}' is enrolled in replication and holds {AcmeTasks.Count} cards.");
    }

    /// <summary>The key of machine <paramref name="index"/> in the demo tree.</summary>
    /// <param name="index">The machine's index.</param>
    public static string MachineKey(int index) => $"machine-{index:D3}";

    /// <summary>The tenant-scoped id of a tenant's <c>orders</c> tree.</summary>
    /// <param name="tenant">The tenant id.</param>
    public static string OrdersTree(string tenant) => LatticeTenantTrees.Compose(TenantId.Parse(tenant), SampleIdentities.TenantOrdersTree);

    /// <summary>The tenant-scoped id of the task board's <c>tasks</c> tree, as installed in <paramref name="tenant"/>.</summary>
    /// <param name="tenant">The tenant id.</param>
    public static string TaskBoardTree(string tenant) =>
        LatticeTenantTrees.Compose(TenantId.Parse(tenant), $"a/{TaskBoardApp.Slug}/tasks");

    /// <summary>The key of the task card <paramref name="id"/>.</summary>
    /// <param name="id">The card id.</param>
    public static string TaskKey(string id) => "tasks/" + id;

    /// <summary>The Basic credential token the sample's authenticator resolves to <paramref name="subject"/>.</summary>
    /// <param name="subject">The subject to sign in as.</param>
    public static string BasicToken(string subject) =>
        Convert.ToBase64String(Encoding.UTF8.GetBytes(subject + ":sample"));

    private static async Task SeedAccessAsync(IServiceProvider services, CancellationToken cancellationToken)
    {
        using var _ = LatticeSystemOrigin.Enter();
        var membership = services.GetRequiredService<ILatticeMembershipDirectory>();

        // Each group gets a record as well as its members: a membership edge alone
        // makes no group, so Access > Groups would list none and could not tell
        // that a seeded id is already taken.
        await membership.UpsertGroupAsync(new MembershipGroup(SampleIdentities.OperatorsGroup, "Floor Operators"), cancellationToken).ConfigureAwait(false);
        await membership.UpsertGroupAsync(new MembershipGroup(SampleIdentities.TaskEditorsGroup, "Task board editors"), cancellationToken).ConfigureAwait(false);
        await membership.UpsertGroupAsync(new MembershipGroup(SampleIdentities.TaskViewersGroup, "Task board viewers"), cancellationToken).ConfigureAwait(false);
        await membership.UpsertGroupAsync(new MembershipGroup(SampleIdentities.VisitorsGroup, "Visitors"), cancellationToken).ConfigureAwait(false);
        await membership.UpsertGroupAsync(new MembershipGroup(SampleIdentities.AcmeEditorsGroup, "Acme task board editors"), cancellationToken).ConfigureAwait(false);
        await membership.AddMemberAsync(SampleIdentities.OperatorsGroup, SampleIdentities.Alice, cancellationToken: cancellationToken).ConfigureAwait(false);

        // The task-board walkthrough's groups (Apps/TaskBoard/README.md): alice
        // edits, bob views, carol is bound to no role, and acme's admin edits
        // acme's board.
        await membership.AddMemberAsync(SampleIdentities.TaskEditorsGroup, SampleIdentities.Alice, cancellationToken: cancellationToken).ConfigureAwait(false);
        await membership.AddMemberAsync(SampleIdentities.TaskViewersGroup, SampleIdentities.Bob, cancellationToken: cancellationToken).ConfigureAwait(false);
        await membership.AddMemberAsync(SampleIdentities.VisitorsGroup, SampleIdentities.Carol, cancellationToken: cancellationToken).ConfigureAwait(false);
        await membership.AddMemberAsync(SampleIdentities.AcmeEditorsGroup, SampleIdentities.AcmeAdmin, cancellationToken: cancellationToken).ConfigureAwait(false);

        var policy = services.GetRequiredService<ILatticeAuthorizationPolicyStore>();
        await policy.PutRuleAsync(
            new LatticeAuthorizationRule(
                ruleId: OperatorsReadRuleId,
                subject: LatticeSubjectSelector.Group(SampleIdentities.OperatorsGroup),
                scope: LatticeScope.Tree(SampleIdentities.FactoryFloorTree),
                operations: LatticeOperation.Read | LatticeOperation.RangeRead,
                effect: LatticeEffect.Allow),
            cancellationToken).ConfigureAwait(false);
    }

    private static async Task SeedTenancyAsync(SampleRegion region, SampleRegion peer, CancellationToken cancellationToken)
    {
        var services = region.Services;

        // Each tenant may use both regions. acme is resident in both and Online
        // in both, as a tenant served by the two regions it replicates between
        // is: a receiver refuses a tenant's writes in a region where it is not
        // Online, so acme's task board replicates only because both are. globex
        // keeps no residency, which serves it in every region, so the sample
        // shows both states.
        var allowed = new[] { SampleIdentities.EastRegion, SampleIdentities.WestRegion };
        var tenants = new[]
        {
            (Tenant: SampleIdentities.AcmeTenant, Admin: SampleIdentities.AcmeAdmin, MaxKeys: 500L),
            (Tenant: SampleIdentities.GlobexTenant, Admin: SampleIdentities.GlobexAdmin, MaxKeys: 200L),
        };

        // The lifecycle, quota, region and grant calls run as the platform
        // operator through the same facades the Tenancy area calls. The operator
        // administers each tenant beside the tenant's own admin, as a tenant the
        // operator creates without naming admins would: the tenant directory
        // lists the tenants its caller administers.
        using (LatticeCredentialContext.Use(BasicToken(SampleIdentities.Administrator), scheme: DemoBasicAuthenticator.Scheme))
        {
            var admin = services.GetRequiredService<ILatticeTenantAdmin>();
            var regions = services.GetRequiredService<ILatticeTenantRegionAdmin>();
            foreach (var (tenant, tenantAdmin, maxKeys) in tenants)
            {
                await admin.CreateTenantAsync(tenant, [tenantAdmin, SampleIdentities.Administrator], cancellationToken).ConfigureAwait(false);
                await admin.SetTenantQuotasAsync(
                    tenant,
                    new TenantQuotasDescriptor { MaxKeys = maxKeys, MaxTreeCount = 10, BurstPercent = 20 },
                    cancellationToken).ConfigureAwait(false);
                await regions.AuthorizeAllowedRegionsAsync(tenant, allowed, cancellationToken).ConfigureAwait(false);
            }

            // The first resident region has no tenant trees to recover. Establish
            // it as Online before enrolling tenant trees and adding the peer, so
            // every non-empty peer bootstrap has a verified Online source.
            await regions.SetResidencyAsync(
                SampleIdentities.AcmeTenant, [SampleIdentities.EastRegion], cancellationToken).ConfigureAwait(false);

            var grants = services.GetRequiredService<ILatticeTenantGrantAdmin>();

            // A grant's scope is the granting tenant's full tree id: the tenant gate
            // matches it against the t/{tenant}/... id a crossing reads.
            await grants.OfferGrantAsync(
                SampleIdentities.AcmeTenant,
                SampleIdentities.GlobexTenant,
                OrdersTree(SampleIdentities.AcmeTenant),
                TenantGrantAccess.Read,
                cancellationToken).ConfigureAwait(false);
        }

        await WaitForTenantRegionAsync(
            region, SampleIdentities.AcmeTenant, SampleIdentities.EastRegion, TenantRegionStatus.Online, cancellationToken)
            .ConfigureAwait(false);

        await EnrolTenantTreesAsync(region, peer, cancellationToken).ConfigureAwait(false);
        using (LatticeCredentialContext.Use(BasicToken(SampleIdentities.Administrator), scheme: DemoBasicAuthenticator.Scheme))
        {
            await services.GetRequiredService<ILatticeTenantRegionAdmin>()
                .SetResidencyAsync(SampleIdentities.AcmeTenant, allowed, cancellationToken).ConfigureAwait(false);
        }

        // The added peer reaches Online through automatic backfill; seed writes
        // only after its verified bootstrap is complete.
        await WaitForTenantSeedAsync(peer, cancellationToken).ConfigureAwait(false);

        // Each tenant's orders, and the rule that lets its admin work with them:
        // tenancy scopes what an admin may reach, and the deny-by-default gate
        // still needs a grant for the data itself.
        using (LatticeSystemOrigin.Enter())
        {
            var grains = services.GetRequiredService<IGrainFactory>();
            var policy = services.GetRequiredService<ILatticeAuthorizationPolicyStore>();
            var number = 1000;
            foreach (var (tenant, tenantAdmin, _) in tenants)
            {
                var treeId = OrdersTree(tenant);
                var orders = grains.GetGrain<ILattice>(treeId);
                for (var i = 0; i < OrdersPerTenant; i++)
                {
                    number++;
                    await orders.SetAsync($"order-{number}", Encoding.UTF8.GetBytes($"{{\"customer\":\"{tenant}\",\"lines\":{i + 1}}}"), cancellationToken)
                        .ConfigureAwait(false);
                }

                await policy.PutRuleAsync(
                    new LatticeAuthorizationRule(
                        ruleId: $"{tenant}-admin-orders",
                        subject: LatticeSubjectSelector.User(tenantAdmin),
                        scope: LatticeScope.Tree(treeId),
                        operations: LatticeOperation.Read | LatticeOperation.RangeRead | LatticeOperation.Write | LatticeOperation.Delete,
                        effect: LatticeEffect.Allow),
                    cancellationToken).ConfigureAwait(false);
            }

            // The grant opens the tenant boundary between acme and globex; the
            // deny-by-default gate still decides who may read. globex's admin may
            // read acme's orders, and once globex approves the grant the crossing
            // is admitted.
            await policy.PutRuleAsync(
                new LatticeAuthorizationRule(
                    ruleId: SharedOrdersRuleId,
                    subject: LatticeSubjectSelector.User(SampleIdentities.GlobexAdmin),
                    scope: LatticeScope.Tree(OrdersTree(SampleIdentities.AcmeTenant)),
                    operations: LatticeOperation.Read | LatticeOperation.RangeRead,
                    effect: LatticeEffect.Allow),
                cancellationToken).ConfigureAwait(false);

            // globex's invoices, where a Platform rule shadows globex's own rule.
            var invoices = grains.GetGrain<ILattice>(InvoicesTree);
            for (var i = 0; i < InvoicesPerTenant; i++)
            {
                await invoices.SetAsync($"invoice-{i + 1:D3}", Encoding.UTF8.GetBytes($"{{\"customer\":\"{SampleIdentities.GlobexTenant}\",\"total\":{(i + 1) * 100}}}"), cancellationToken)
                    .ConfigureAwait(false);
            }

            // The Platform layer is decided first: this cluster rule withholds
            // globex's invoices from alice whatever globex's own rules say.
            await policy.PutRuleAsync(
                new LatticeAuthorizationRule(
                    ruleId: GlobexInvoicesPlatformRuleId,
                    subject: LatticeSubjectSelector.User(SampleIdentities.Alice),
                    scope: LatticeScope.Tree(InvoicesTree),
                    operations: LatticeOperation.Read | LatticeOperation.RangeRead,
                    effect: LatticeEffect.Deny),
                cancellationToken).ConfigureAwait(false);
        }

        await SeedGlobexAccessAsync(services, cancellationToken).ConfigureAwait(false);
    }

    private static async Task WaitForTenantSeedAsync(SampleRegion peer, CancellationToken cancellationToken)
    {
        var registry = peer.Services.GetRequiredService<ITenantRegistry>();
        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        deadline.CancelAfter(EnrolmentBudget);
        using var poll = new PeriodicTimer(TimeSpan.FromMilliseconds(100));
        try
        {
            do
            {
                var acme = await registry.GetAsync(TenantId.Parse(SampleIdentities.AcmeTenant), deadline.Token).ConfigureAwait(false);
                var globex = await registry.GetAsync(TenantId.Parse(SampleIdentities.GlobexTenant), deadline.Token).ConfigureAwait(false);
                if (acme?.GetRegionStatus(SampleIdentities.EastRegion) == TenantRegionStatus.Online
                    && acme.GetRegionStatus(SampleIdentities.WestRegion) == TenantRegionStatus.Online
                    && globex is not null)
                {
                    return;
                }
            }
            while (await poll.WaitForNextTickAsync(deadline.Token).ConfigureAwait(false));
        }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
        {
            var acme = await registry.GetAsync(TenantId.Parse(SampleIdentities.AcmeTenant), CancellationToken.None).ConfigureAwait(false);
            var globex = await registry.GetAsync(TenantId.Parse(SampleIdentities.GlobexTenant), CancellationToken.None).ConfigureAwait(false);
            var acmeStatuses = acme is null
                ? "missing"
                : string.Join(", ", acme.RegionStatusEntries.Select(entry => $"{entry.Key}={entry.Value}"));
            var globexStatuses = globex is null
                ? "missing"
                : string.Join(", ", globex.RegionStatusEntries.Select(entry => $"{entry.Key}={entry.Value}"));
            ReplicationConfigReport config;
            using (LatticeSystemOrigin.Enter())
            {
                config = await peer.Services.GetRequiredService<ILatticeReplicationControl>()
                    .GetReplicationConfigAsync(CancellationToken.None).ConfigureAwait(false);
            }

            var registryTree = config.Trees.FirstOrDefault(tree =>
                string.Equals(tree.TreeId, "sys-tenant-registry", StringComparison.Ordinal));
            var bootstrap = await peer.Services.GetRequiredService<ILatticeBootstrapCoordinator>()
                .GetStatusAsync("sys-tenant-registry").ConfigureAwait(false);
            throw new TimeoutException(
                $"Region '{peer.Id}' has not observed the seeded tenant registry within {EnrolmentBudget.TotalSeconds:0}s. "
                + $"{peer.Id} records: acme=[{acmeStatuses}], globex=[{globexStatuses}]; "
                + $"registry replication enabled={registryTree?.Enabled}, ambiguous={registryTree?.Ambiguous}; "
                + $"bootstrap phase={bootstrap.Phase}, source={bootstrap.SourceClusterId}, "
                + $"read-fenced={bootstrap.ReadFenced}, entries-applied={bootstrap.EntriesApplied}. "
                + "Check sys-tenant-registry replication.");
        }
    }

    private static async Task WaitForTenantRegionAsync(
        SampleRegion region,
        string tenantId,
        string regionId,
        TenantRegionStatus expectedStatus,
        CancellationToken cancellationToken)
    {
        var registry = region.Services.GetRequiredService<ITenantRegistry>();
        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        deadline.CancelAfter(EnrolmentBudget);
        using var poll = new PeriodicTimer(TimeSpan.FromMilliseconds(100));
        try
        {
            do
            {
                var tenant = await registry.GetAsync(TenantId.Parse(tenantId), deadline.Token).ConfigureAwait(false);
                if (tenant?.GetRegionStatus(regionId) == expectedStatus)
                {
                    return;
                }
            }
            while (await poll.WaitForNextTickAsync(deadline.Token).ConfigureAwait(false));
        }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
        {
            var tenant = await registry.GetAsync(TenantId.Parse(tenantId), CancellationToken.None).ConfigureAwait(false);
            throw new TimeoutException(
                $"Tenant '{tenantId}' did not reach {expectedStatus} in region '{regionId}' within "
                + $"{EnrolmentBudget.TotalSeconds:0}s; current status is {tenant?.GetRegionStatus(regionId)}.");
        }
    }

    /// <summary>
    /// Seeds globex's delegated tenant access through the same tenant directory and
    /// tenant policy facades Access calls: its own group <c>t/globex/operators</c>
    /// holding alice, a member set holding that group, and globex rules granting it
    /// Read on its orders and its invoices (the second shadowed by the Platform rule
    /// above). Runs as the operator, who also administers globex.
    /// </summary>
    private static async Task SeedGlobexAccessAsync(IServiceProvider services, CancellationToken cancellationToken)
    {
        const string Globex = SampleIdentities.GlobexTenant;
        const string Group = SampleIdentities.GlobexOperatorsGroup;

        using var _ = LatticeCredentialContext.Use(BasicToken(SampleIdentities.Administrator), scheme: DemoBasicAuthenticator.Scheme);
        var directory = services.GetRequiredService<ILatticeTenantDirectoryAdmin>();
        var policy = services.GetRequiredService<ILatticeTenantPolicyAdmin>();

        // The group record first: a membership edge alone makes no group.
        await directory.UpsertGroupAsync(Globex, new TenantGroupDescriptor { Name = Group, DisplayName = "Globex operators" }, cancellationToken)
            .ConfigureAwait(false);
        await directory.AddGroupMemberAsync(Globex, Group, SampleIdentities.Alice, TenantSubjectKind.User, cancellationToken).ConfigureAwait(false);
        await directory.AddMemberAsync(Globex, Group, TenantSubjectKind.TenantGroup, cancellationToken).ConfigureAwait(false);

        foreach (var (ruleId, tree) in new[]
        {
            (GlobexOrdersRuleId, SampleIdentities.TenantOrdersTree),
            (GlobexInvoicesRuleId, SampleIdentities.GlobexInvoicesTree),
        })
        {
            await policy.PutRuleAsync(
                Globex,
                new TenantRuleDraft
                {
                    RuleId = ruleId,
                    SubjectId = Group,
                    SubjectKind = TenantSubjectKind.TenantGroup,
                    ScopeKind = TenantRuleScopeKind.Tree,
                    TreeName = tree,
                    Operations = LatticeOperation.Read | LatticeOperation.RangeRead,
                    Effect = LatticeEffect.Allow,
                },
                cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Promotes <paramref name="regionId"/> of <paramref name="tenant"/> one legal
    /// lifecycle step at a time until it is Online, as an operator of the hosting
    /// deployment does: Provisioning, then Backfilling, then Online.
    /// </summary>
    /// <param name="registry">The region's tenant registry.</param>
    /// <param name="tenant">The tenant.</param>
    /// <param name="regionId">The region to promote.</param>
    /// <param name="writerId">The writer id stamped on each promotion: the promoting region's id.</param>
    /// <param name="cancellationToken">Cancels the promotion.</param>
    /// <returns>The region's committed status once no further step applies.</returns>
    /// <summary>
    /// Installs and enables the task board in <paramref name="tenant"/> through
    /// the app registry and activation pipeline - the engine the Apps area's
    /// install drives - consenting to exactly the bridge grants its manifest
    /// requests. Activation provisions the app's tree, writes its role rules and
    /// enrols the tree its manifest declares for replication.
    /// </summary>
    /// <remarks>
    /// The app is resolved from the in-image source and recorded under the
    /// provenance that source vouches for, exactly as an install from Apps &gt;
    /// Catalogue records it - never the manifest's self-declared publisher. Its
    /// trees are claimed under that publisher, so a later upgrade from the same
    /// source is the same owner and is not refused by the ownership claim check.
    /// </remarks>
    /// <returns>The effective id of the app's <c>tasks</c> tree.</returns>
    private static async Task<string> InstallTaskBoardAsync(IServiceProvider services, TenantId tenant, CancellationToken cancellationToken)
    {
        var slug = AppSlug.Parse(TaskBoardApp.Slug);
        var resolved = await services.GetRequiredService<AppSourceSet>()
            .ResolveAsync(slug, version: null, InImageAppSource.SourceKey, cancellationToken)
            .ConfigureAwait(false);
        if (!resolved.IsResolved || resolved.Manifest is not { } manifest || resolved.Provenance is not { } provenance)
        {
            throw new InvalidOperationException($"The in-image source did not resolve '{slug}': {resolved.Status}.");
        }

        using var _ = LatticeSystemOrigin.Enter();
        var registry = services.GetRequiredService<IAppRegistry>();
        var installed = await registry.InstallAsync(
            new AppRegistryInstallRequest
            {
                Tenant = tenant,
                Identity = new AppIdentity { Slug = slug, Version = manifest.Identity.Version, Provenance = provenance },
                Ceiling = AppCapabilityCeiling.Structural(
                    LatticeOperation.Read | LatticeOperation.RangeRead | LatticeOperation.Write | LatticeOperation.Delete),
                RoleBindings = [AppRoleBinding.Create("editor", SampleIdentities.AcmeEditorsGroup)],
                BridgeConsent = AppUiBridgeRequest.FromManifest(manifest),
            },
            cancellationToken).ConfigureAwait(false);
        if (!installed.Succeeded)
        {
            throw new InvalidOperationException($"Installing '{slug}' in tenant '{tenant}' failed: {installed.Error} {installed.Message}");
        }

        var enabled = await services.GetRequiredService<IAppActivationPipeline>()
            .EnableAsync(tenant, slug, cancellationToken)
            .ConfigureAwait(false);
        if (!enabled.Succeeded)
        {
            throw new InvalidOperationException($"Enabling '{slug}' in tenant '{tenant}' failed: {enabled.Failure} "
                + string.Join("; ", enabled.Diagnostics.Select(diagnostic => diagnostic.Message)));
        }

        return TaskBoardTree(tenant.Value);
    }
}

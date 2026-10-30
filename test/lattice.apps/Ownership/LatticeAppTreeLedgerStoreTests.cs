using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Serialization;

namespace Orleans.Lattice.Apps.Tests;

/// <summary>
/// Unit tests for the production ownership seams over substituted grains:
/// <see cref="LatticeAppTreeLedgerStore"/> addresses the reserved <c>sys-app-trees</c> tree under
/// system origin in the Orleans wire format, and <see cref="LatticeAppTreeFacts"/> reads the core
/// tree registry under system origin.
/// </summary>
[TestFixture]
public sealed class LatticeAppTreeLedgerStoreTests
{
    private ServiceProvider _services = null!;
    private Serializer<AppTreeClaim> _serializer = null!;
    private ILattice _lattice = null!;
    private ILatticeRegistry _registry = null!;
    private IGrainFactory _grainFactory = null!;
    private LatticeAppTreeLedgerStore _store = null!;

    private static AppTreeClaim Claim(string slug = "crm") => new()
    {
        Tenant = TenantId.Default,
        Slug = AppSlug.Parse(slug),
        Publisher = "first-party",
        Kind = AppTreeClaimKind.Structural,
    };

    [SetUp]
    public void SetUp()
    {
        _services = new ServiceCollection()
            .AddSerializer(builder => builder
                .AddAssembly(typeof(AppTreeClaim).Assembly)
                .AddAssembly(typeof(TenantId).Assembly))
            .BuildServiceProvider();
        _serializer = _services.GetRequiredService<Serializer<AppTreeClaim>>();
        _lattice = Substitute.For<ILattice>();
        _registry = Substitute.For<ILatticeRegistry>();
        _grainFactory = Substitute.For<IGrainFactory>();
        _grainFactory.GetGrain<ILattice>(AppRegistryTreeNames.TreeLedgerTree, Arg.Any<string?>()).Returns(_lattice);
        _grainFactory.GetGrain<ILatticeRegistry>(Arg.Any<string>(), Arg.Any<string?>()).Returns(_registry);
        _store = new LatticeAppTreeLedgerStore(_grainFactory, _serializer);
    }

    [TearDown]
    public void TearDown() => _services.Dispose();

    [Test]
    public void The_ledger_tree_is_under_the_control_plane_isolated_app_prefix()
    {
        Assert.That(AppRegistryTreeNames.TreeLedgerTree, Is.EqualTo("sys-app-trees"));
        Assert.That(AppRegistryTreeNames.TreeLedgerTree, Does.StartWith(LatticeConstants.AppRegistryTreePrefix));
    }

    [Test]
    public async Task GetAsync_reads_under_system_origin_and_deserializes()
    {
        var version = new HybridLogicalClock { WallClockTicks = 3 };
        var observed = false;
        _lattice.GetWithVersionAsync("a/crm/contacts", Arg.Any<CancellationToken>()).Returns(_ =>
        {
            observed = LatticeSystemOrigin.IsActive;
            return Task.FromResult(new VersionedValue { Value = _serializer.SerializeToArray(Claim()), Version = version });
        });

        var read = await _store.GetAsync("a/crm/contacts", CancellationToken.None);

        Assert.That(observed, Is.True);
        Assert.That(LatticeSystemOrigin.IsActive, Is.False);
        Assert.That(read.Claim, Is.EqualTo(Claim()));
        Assert.That(read.Version, Is.EqualTo(version));
    }

    [Test]
    public async Task GetAsync_miss_returns_null_at_version_zero()
    {
        _lattice.GetWithVersionAsync("a/crm/contacts", Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(new VersionedValue { Value = null, Version = new HybridLogicalClock { WallClockTicks = 9 } }));

        var read = await _store.GetAsync("a/crm/contacts", CancellationToken.None);

        Assert.That(read.Claim, Is.Null);
        Assert.That(read.Version, Is.EqualTo(HybridLogicalClock.Zero));
    }

    [Test]
    public async Task TrySetAsync_writes_conditionally_under_system_origin()
    {
        var expected = new HybridLogicalClock { WallClockTicks = 4 };
        byte[]? written = null;
        var observed = false;
        _lattice.SetIfVersionAsync("a/crm/contacts", Arg.Any<byte[]>(), expected, Arg.Any<CancellationToken>()).Returns(call =>
        {
            observed = LatticeSystemOrigin.IsActive;
            written = call.ArgAt<byte[]>(1);
            return Task.FromResult(false);
        });

        var applied = await _store.TrySetAsync("a/crm/contacts", Claim(), expected, CancellationToken.None);

        Assert.That(applied, Is.False);
        Assert.That(observed, Is.True);
        Assert.That(_serializer.Deserialize(written!), Is.EqualTo(Claim()));
    }

    [Test]
    public async Task ScanAsync_deserializes_every_entry_under_system_origin()
    {
        var observed = false;
        _lattice.EntriesAsync(null, null, false, Arg.Any<bool?>(), Arg.Any<CancellationToken>()).Returns(_ =>
        {
            observed = LatticeSystemOrigin.IsActive;
            return Entries(("a/billing/x", Claim("billing")), ("a/crm/y", Claim()));
        });

        var entries = await _store.ScanAsync(CancellationToken.None).ToListAsync();

        Assert.That(observed, Is.True);
        Assert.That(entries.Select(e => (e.Key, e.Value.Slug.Value)), Is.EqualTo(new[] { ("a/billing/x", "billing"), ("a/crm/y", "crm") }));
    }

    [Test]
    public async Task Facts_read_the_core_registry_under_system_origin()
    {
        var facts = new LatticeAppTreeFacts(_grainFactory);
        var origins = new List<bool>();
        _registry.ExistsAsync("t").Returns(_ => { origins.Add(LatticeSystemOrigin.IsActive); return Task.FromResult(true); });
        _registry.GetEntryAsync("t").Returns(_ => { origins.Add(LatticeSystemOrigin.IsActive); return Task.FromResult<TreeRegistryEntry?>(new TreeRegistryEntry { DerivedFrom = "src" }); });
        _registry.GetEntryAsync("none").Returns(Task.FromResult<TreeRegistryEntry?>(null));
        _registry.ResolveAsync("t").Returns(_ => { origins.Add(LatticeSystemOrigin.IsActive); return Task.FromResult("p"); });
        _registry.GetAliasesTargetingAsync("p").Returns(_ => { origins.Add(LatticeSystemOrigin.IsActive); return Task.FromResult<IReadOnlyList<string>>(["t"]); });

        Assert.That(await facts.ExistsAsync("t"), Is.True);
        Assert.That((await facts.GetEntryAsync("t"))?.DerivedFrom, Is.EqualTo("src"));
        Assert.That(await facts.GetDerivedFromAsync("t"), Is.EqualTo("src"));
        Assert.That(await facts.GetDerivedFromAsync("none"), Is.Null);
        Assert.That(await facts.ResolveAsync("t"), Is.EqualTo("p"));
        Assert.That(await facts.GetAliasesTargetingAsync("p"), Is.EqualTo(new[] { "t" }));
        Assert.That(origins, Is.EqualTo(new[] { true, true, true, true, true }));
    }

    [TestCase(false)]
    [TestCase(true)]
    public async Task Fresh_claim_reads_one_registry_entry_without_existence_or_alias_resolution_calls(bool adopted)
    {
        var store = new InMemoryAppTreeLedgerStore();
        var ledger = new AppTreeOwnershipLedger(store, new LatticeAppTreeFacts(_grainFactory),
            new InMemoryAppRegistryStore(), new ManualTimeProvider(AppRegistryTestData.Start));
        var owner = new AppTreeOwner(TenantId.Default, AppSlug.Parse("crm"), "first-party");
        _registry.GetAliasesTargetingAsync("t").Returns(Task.FromResult<IReadOnlyList<string>>([]));

        var kind = adopted ? AppTreeClaimKind.Adopted : AppTreeClaimKind.Structural;
        var conflict = await ledger.ClaimAsync(owner, 1, [new("t", "t", kind)], null, CancellationToken.None);

        Assert.That(conflict, Is.Null);
        Assert.That(store.AppliedWrites, Is.EqualTo(1));
        await _registry.Received(1).GetEntryAsync("t");
        await _registry.Received(1).GetAliasesTargetingAsync("t");
        await _registry.DidNotReceiveWithAnyArgs().ExistsAsync(default!);
        await _registry.DidNotReceiveWithAnyArgs().ResolveAsync(default!);
    }

    [Test]
    public async Task Fresh_claim_rereads_facts_after_a_lost_compare_and_set()
    {
        var store = new InMemoryAppTreeLedgerStore();
        var ledger = new AppTreeOwnershipLedger(store, new LatticeAppTreeFacts(_grainFactory),
            new InMemoryAppRegistryStore(), new ManualTimeProvider(AppRegistryTestData.Start));
        var owner = new AppTreeOwner(TenantId.Default, AppSlug.Parse("crm"), "first-party");
        _registry.GetAliasesTargetingAsync("t").Returns(Task.FromResult<IReadOnlyList<string>>([]));
        store.BeforeSet = key =>
        {
            store.Seed(key, Claim() with { Released = true });
            _registry.GetEntryAsync(key).Returns(Task.FromResult<TreeRegistryEntry?>(new TreeRegistryEntry()));
            return Task.CompletedTask;
        };

        var conflict = await ledger.ClaimAsync(owner, 1,
            [new("t", "t", AppTreeClaimKind.Structural)], null, CancellationToken.None);

        Assert.That(conflict?.Reason, Is.EqualTo(AppTreeOwnershipConflictReason.PreExistingUnownedTree));
        Assert.That(store.AppliedWrites, Is.Zero);
        await _registry.Received(2).GetEntryAsync("t");
    }

    [Test]
    public void Fresh_claim_propagates_registry_failure_without_writing_a_claim()
    {
        var store = new InMemoryAppTreeLedgerStore();
        var ledger = new AppTreeOwnershipLedger(store, new LatticeAppTreeFacts(_grainFactory),
            new InMemoryAppRegistryStore(), new ManualTimeProvider(AppRegistryTestData.Start));
        var owner = new AppTreeOwner(TenantId.Default, AppSlug.Parse("crm"), "first-party");
        _registry.GetEntryAsync("t").Returns(Task.FromException<TreeRegistryEntry?>(new InvalidOperationException("unavailable")));

        Assert.ThrowsAsync<InvalidOperationException>(async () => await ledger.ClaimAsync(owner, 1,
            [new("t", "t", AppTreeClaimKind.Structural)], null, CancellationToken.None));
        Assert.That(store.AppliedWrites, Is.Zero);
        Assert.That(LatticeSystemOrigin.IsActive, Is.False);
    }

    [Test]
    public void Constructor_null_arguments_throw()
    {
        Assert.That(() => new LatticeAppTreeLedgerStore(null!, _serializer), Throws.ArgumentNullException);
        Assert.That(() => new LatticeAppTreeLedgerStore(_grainFactory, null!), Throws.ArgumentNullException);
        Assert.That(() => new LatticeAppTreeFacts(null!), Throws.ArgumentNullException);
    }

    private async IAsyncEnumerable<KeyValuePair<string, byte[]>> Entries(params (string Key, AppTreeClaim Claim)[] entries)
    {
        foreach (var (key, claim) in entries)
            yield return new(key, _serializer.SerializeToArray(claim));
        await Task.CompletedTask;
    }
}

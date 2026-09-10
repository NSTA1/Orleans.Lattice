using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Regression coverage for the alias-target authorization gap.
/// <para>
/// A tree alias transplants a logical identity onto another tree's physical
/// shards. Routing resolves the alias and addresses the target's shards
/// directly, while every data-plane access gate on the
/// <see cref="Orleans.Lattice.ILattice"/> facade has already been evaluated
/// against the <em>logical</em> id. Binding tree <c>a</c> to tree <c>b</c>
/// therefore confers unrestricted read and write over every key of <c>b</c>,
/// authorized only as <c>a</c>.
/// </para>
/// <para>
/// <c>ThrowIfAliasEscalatesNamespace</c> closes only the namespace half of that
/// (reserved namespace, system-data crossing, foreign tenant); two ordinary
/// same-namespace ids are indistinguishable to it and passed unconditionally.
/// These tests pin the remaining half: the caller must hold whole-tree control
/// of the alias <b>target</b>, answered by the access gate.
/// </para>
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class AliasTargetAuthorizationIntegrationTests
{
    private const string Attacker = "alias-auth-attacker";
    private const string Victim = "alias-auth-victim";

    private AccessGateKeyFilterClusterFixture _fixture = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new AccessGateKeyFilterClusterFixture();
        await _fixture.InitializeAsync();
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown()
    {
        if (_fixture is not null)
        {
            await _fixture.DisposeAsync();
        }
    }

    [TearDown]
    public void TearDown() => ConfigurableAccessGate.Reset();

    private ILatticeRegistry Registry =>
        _fixture.Cluster.Client.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);

    /// <summary>
    /// Registers both trees with the gate wide open, so a test's own seeding is
    /// never what the assertion observes. The gate is then narrowed per test.
    /// </summary>
    private async Task SeedAsync(string logical, string physical)
    {
        ConfigurableAccessGate.Reset();
        var registry = Registry;
        await registry.RegisterAsync(logical, new TreeRegistryEntry { ShardCount = 1, MaxLeafKeys = 4 });
        await registry.RegisterAsync(physical, new TreeRegistryEntry { ShardCount = 1, MaxLeafKeys = 4 });
    }

    [Test]
    public async Task alias_onto_a_tree_the_caller_does_not_control_is_refused()
    {
        var logical = $"{Attacker}-1";
        var physical = $"{Victim}-1";
        await SeedAsync(logical, physical);

        // The caller owns the logical tree outright and has no rights at all on
        // the target. This is the escalation in its plainest form: both ids are
        // ordinary and same-namespace, so the namespace guard passes them.
        ConfigurableAccessGate.Decide = req =>
            string.Equals(req.TreeId, physical, StringComparison.Ordinal)
                ? LatticeAccessDecision.Deny("not your tree")
                : LatticeAccessDecision.Allow();

        var ex = Assert.ThrowsAsync<LatticeAuthorizationDeniedException>(
            async () => await Registry.SetAliasAsync(logical, physical));

        Assert.Multiple(() =>
        {
            Assert.That(ex!.TreeId, Is.EqualTo(physical),
                "authorization is evaluated against the alias target, not the logical id");
            Assert.That(ex.Operation, Is.EqualTo(LatticeOperation.Admin));
        });
    }

    [Test]
    public async Task refused_alias_is_not_written()
    {
        var logical = $"{Attacker}-2";
        var physical = $"{Victim}-2";
        await SeedAsync(logical, physical);

        ConfigurableAccessGate.Decide = req =>
            string.Equals(req.TreeId, physical, StringComparison.Ordinal)
                ? LatticeAccessDecision.Deny("not your tree")
                : LatticeAccessDecision.Allow();

        Assert.ThrowsAsync<LatticeAuthorizationDeniedException>(
            async () => await Registry.SetAliasAsync(logical, physical));

        ConfigurableAccessGate.Reset();
        var entry = await Registry.GetEntryAsync(logical);

        Assert.That(entry?.PhysicalTreeId, Is.Null,
            "the check runs before anything is written, so a refused alias leaves no trace");
    }

    [Test]
    public async Task alias_onto_a_tree_the_caller_controls_is_allowed()
    {
        var logical = $"{Attacker}-3";
        var physical = $"{Victim}-3";
        await SeedAsync(logical, physical);

        // Whole-tree control of the target: an unrestricted allow.
        ConfigurableAccessGate.Decide = static _ => LatticeAccessDecision.Allow();

        await Registry.SetAliasAsync(logical, physical);
        var entry = await Registry.GetEntryAsync(logical);

        Assert.That(entry?.PhysicalTreeId, Is.EqualTo(physical));
    }

    [Test]
    public async Task alias_with_only_a_key_filtered_allow_on_the_target_is_refused()
    {
        var logical = $"{Attacker}-4";
        var physical = $"{Victim}-4";
        await SeedAsync(logical, physical);

        // A partial-coverage allow is not whole-tree control. An alias confers
        // unrestricted access to every key the target holds, now and in future,
        // so there is nothing for a per-key filter to narrow: fail closed.
        ConfigurableAccessGate.Decide = req =>
            string.Equals(req.TreeId, physical, StringComparison.Ordinal)
                ? LatticeAccessDecision.Filtered(static k => k.StartsWith("public/", StringComparison.Ordinal))
                : LatticeAccessDecision.Allow();

        Assert.ThrowsAsync<LatticeAuthorizationDeniedException>(
            async () => await Registry.SetAliasAsync(logical, physical));
    }

    [Test]
    public async Task control_of_the_logical_tree_alone_does_not_authorize_the_alias()
    {
        var logical = $"{Attacker}-5";
        var physical = $"{Victim}-5";
        await SeedAsync(logical, physical);

        // Allow *only* the logical tree. A check evaluated against the logical
        // id would pass here, which is exactly the bug: the caller's rights on
        // the tree they own say nothing about the tree they are pointing it at.
        ConfigurableAccessGate.Decide = req =>
            string.Equals(req.TreeId, logical, StringComparison.Ordinal)
                ? LatticeAccessDecision.Allow()
                : LatticeAccessDecision.Deny("no rights on the target");

        var ex = Assert.ThrowsAsync<LatticeAuthorizationDeniedException>(
            async () => await Registry.SetAliasAsync(logical, physical));

        Assert.That(ex!.TreeId, Is.EqualTo(physical));
    }

    [Test]
    public async Task a_system_origin_turn_is_exempt()
    {
        var logical = $"{Attacker}-6";
        var physical = $"{Victim}-6";
        await SeedAsync(logical, physical);

        // Library-internal maintenance derives a physical id from the logical
        // one (resize, resharding, schema remediation, shadow restore) and is
        // already gated at its own entry point. It must not be self-blocked -
        // the same exemption the namespace guard takes.
        ConfigurableAccessGate.Decide = static _ => LatticeAccessDecision.Deny("deny everything");

        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            await Registry.SetAliasAsync(logical, physical);
        }

        ConfigurableAccessGate.Reset();
        var entry = await Registry.GetEntryAsync(logical);

        Assert.That(entry?.PhysicalTreeId, Is.EqualTo(physical));
    }

    [Test]
    public async Task removing_an_alias_is_not_gated_on_the_target()
    {
        var logical = $"{Attacker}-7";
        var physical = $"{Victim}-7";
        await SeedAsync(logical, physical);

        ConfigurableAccessGate.Decide = static _ => LatticeAccessDecision.Allow();
        await Registry.SetAliasAsync(logical, physical);

        // Removal only ever *reduces* the caller's reach - it returns the
        // logical id to its own shards - so it acquires no authority over the
        // target and is deliberately left ungated.
        ConfigurableAccessGate.Decide = req =>
            string.Equals(req.TreeId, physical, StringComparison.Ordinal)
                ? LatticeAccessDecision.Deny("not your tree")
                : LatticeAccessDecision.Allow();

        await Registry.RemoveAliasAsync(logical);

        ConfigurableAccessGate.Reset();
        var entry = await Registry.GetEntryAsync(logical);

        Assert.That(entry?.PhysicalTreeId, Is.Null);
    }

    [Test]
    public async Task ordinary_registry_traffic_is_unaffected_by_the_alias_check()
    {
        // The fix is scoped to the alias verb. Registration, catalogue reads and
        // per-tree configuration are legitimately driven by first-party callers
        // and must keep working against a gate that denies the alias target.
        var tree = $"{Attacker}-8";
        ConfigurableAccessGate.Reset();
        await Registry.RegisterAsync(tree, new TreeRegistryEntry { ShardCount = 1, MaxLeafKeys = 4 });

        ConfigurableAccessGate.Decide = static _ => LatticeAccessDecision.Deny("deny everything");

        var entry = await Registry.GetEntryAsync(tree);
        var ids = await Registry.GetAllTreeIdsAsync();

        Assert.Multiple(() =>
        {
            Assert.That(entry, Is.Not.Null);
            Assert.That(ids, Does.Contain(tree));
        });
    }
}

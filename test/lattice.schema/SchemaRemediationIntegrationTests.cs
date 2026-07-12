using System.Text;

namespace Orleans.Lattice.Schema.Tests;

/// <summary>
/// End-to-end integration tests for the background schema-remediation shadow build
/// and physical cutover on a real single-silo <see cref="Orleans.TestingHost.TestCluster"/>.
/// Prove that a successful remediation rewrites every value, repoints the logical
/// tree to the remediated destination, installs the target policy, and enforces it
/// on subsequent writes - and that an un-remediable value aborts with the original
/// tree completely untouched (no data change, no policy). Phases are driven
/// synchronously via the coordinator's <c>StartAsync</c>; there is no timing
/// dependence.
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class SchemaRemediationIntegrationTests
{
    private SchemaRemediationClusterFixture _fixture = null!;

    private IGrainFactory Grains => _fixture.Cluster.GrainFactory;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new SchemaRemediationClusterFixture();
        await _fixture.InitializeAsync();
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown() => await _fixture.DisposeAsync();

    private static byte[] Utf8(string s) => Encoding.UTF8.GetBytes(s);

    private static string Text(byte[]? value) => value is null ? string.Empty : Encoding.UTF8.GetString(value);

    [Test]
    public async Task Remediation_rewrites_values_cuts_over_and_enforces_the_new_policy()
    {
        const string treeId = "orders-success";
        var lattice = Grains.GetGrain<ILattice>(treeId);
        await lattice.SetAsync("k1", Utf8("{\"v\":1}"));
        await lattice.SetAsync("k2", Utf8("{\"v\":2}"));

        // Add a "status" member to every document, and require the result to be JSON.
        var transform = LatticeValueTransform.Passthrough(
            LatticeValueTransform.SetMember("status", LatticeValueTransform.Const(LatticeConstant.Text("ok"))));
        var policy = new LatticeSchemaPolicy(new[] { LatticeSchemaRule.Json() });

        var remediation = Grains.GetGrain<ILatticeSchemaRemediationGrain>(treeId);
        var report = await remediation.StartAsync(transform, policy);

        Assert.That(report.Succeeded, Is.True);
        Assert.That(report.ScannedCount, Is.EqualTo(2));

        // Reads on the logical tree now serve the remediated (rewritten) values.
        var k1 = Text(await lattice.GetAsync("k1"));
        var k2 = Text(await lattice.GetAsync("k2"));
        Assert.That(k1, Does.Contain("status").And.Contain("ok"));
        Assert.That(k2, Does.Contain("status").And.Contain("ok"));

        // The target policy is installed and enforced: a non-JSON write is rejected.
        Assert.That(
            async () => await lattice.SetAsync("k3", Utf8("not-json")),
            Throws.TypeOf<LatticeSchemaViolationException>());

        // A conforming write is accepted.
        await lattice.SetAsync("k3", Utf8("{\"v\":3}"));
        Assert.That(Text(await lattice.GetAsync("k3")), Does.Contain("\"v\":3"));

        // Status reflects the completed remediation.
        var status = await remediation.GetStatusAsync();
        Assert.That(status.Succeeded, Is.True);
    }

    [Test]
    public async Task Remediation_abort_leaves_the_original_tree_untouched()
    {
        const string treeId = "orders-abort";
        var lattice = Grains.GetGrain<ILattice>(treeId);
        await lattice.SetAsync("k1", Utf8("{\"v\":1}"));
        await lattice.SetAsync("k2", Utf8("{\"v\":2}"));

        // A policy no value can satisfy: max 3 bytes. The dry-run gate aborts.
        var policy = new LatticeSchemaPolicy(new[] { LatticeSchemaRule.MaxLength(3) });

        var remediation = Grains.GetGrain<ILatticeSchemaRemediationGrain>(treeId);
        var report = await remediation.StartAsync(LatticeValueTransform.Passthrough(), policy);

        Assert.That(report.DidAbort, Is.True);
        Assert.That(report.OffendingKey, Is.Not.Null);

        // Original data is intact and unchanged.
        Assert.That(Text(await lattice.GetAsync("k1")), Is.EqualTo("{\"v\":1}"));
        Assert.That(Text(await lattice.GetAsync("k2")), Is.EqualTo("{\"v\":2}"));

        // No policy was installed: a write that would violate the aborted policy
        // (longer than 3 bytes) is still accepted, proving the tree is ungoverned.
        await lattice.SetAsync("k3", Utf8("{\"v\":3}"));
        Assert.That(Text(await lattice.GetAsync("k3")), Is.EqualTo("{\"v\":3}"));
    }

    [Test]
    public async Task Remediation_status_is_idle_for_a_tree_that_was_never_remediated()
    {
        var remediation = Grains.GetGrain<ILatticeSchemaRemediationGrain>("orders-never");
        var status = await remediation.GetStatusAsync();
        Assert.That(status.Phase, Is.EqualTo(LatticeSchemaRemediationPhase.Idle));
    }
}

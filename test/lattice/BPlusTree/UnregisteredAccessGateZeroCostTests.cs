using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// End-to-end proof that a cluster with neither the membership nor the
/// authorization add-on registered carries no authorization control point on the
/// data path: the core <see cref="LatticeServiceCollectionExtensions"/>
/// registration installs only the allow-all <see cref="NullLatticeAccessGate"/>, so
/// every operation runs
/// exactly as it did before the authorization layer existed. An anonymous caller
/// with no policy performs the full operation matrix without a single denial,
/// which is the "opt-in, zero cost when disabled" contract the enforcing gate is
/// layered on top of.
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class UnregisteredAccessGateZeroCostTests
{
    private ClusterFixture _fixture = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new ClusterFixture();
        await _fixture.InitializeAsync();
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown()
    {
        await _fixture.DisposeAsync();
    }

    private static byte[] Bytes(string value) => Encoding.UTF8.GetBytes(value);

    private ILattice Lattice(string treeId) => _fixture.Cluster.Client.GetGrain<ILattice>(treeId);

    [Test]
    public void With_no_auth_registered_the_resolved_gate_is_the_allow_all_null_gate()
    {
        var siloServices = _fixture.Cluster.Silos
            .OfType<InProcessSiloHandle>()
            .First()
            .SiloHost.Services;

        var gate = siloServices.GetRequiredService<ILatticeAccessGate>();

        Assert.That(gate, Is.TypeOf<NullLatticeAccessGate>(),
            "a cluster without the authorization add-on must resolve the core allow-all gate, so no policy check is on the path");
    }

    [Test]
    public async Task With_no_auth_registered_every_operation_class_runs_unchanged_for_an_anonymous_caller()
    {
        const string tree = "zerocost-ops";
        var lattice = Lattice(tree);

        // No AsSubject scope, no rules authored: an anonymous caller drives the
        // full operation matrix and nothing is denied.
        await lattice.SetAsync("a:1", Bytes("1"));
        await lattice.SetAsync("a:2", Bytes("2"));
        await lattice.SetAsync("b:1", Bytes("3"));

        Assert.That(Encoding.UTF8.GetString((await lattice.GetAsync("a:1"))!), Is.EqualTo("1"));
        Assert.That(await lattice.ExistsAsync("a:1"), Is.True);

        var many = await lattice.GetManyAsync(new List<string> { "a:1", "b:1" });
        Assert.That(many.Keys, Is.EquivalentTo(new[] { "a:1", "b:1" }),
            "GetMany returns every key with no gate to prune it");

        var scanned = new List<string>();
        await foreach (var key in lattice.KeysAsync())
        {
            scanned.Add(key);
        }

        Assert.That(scanned, Is.EquivalentTo(new[] { "a:1", "a:2", "b:1" }),
            "a range scan observes every key with no key filter applied");

        Assert.That(await lattice.DeleteAsync("b:1"), Is.True);
        Assert.That(await lattice.GetAsync("b:1"), Is.Null);
    }

    [Test]
    public async Task With_no_auth_registered_the_metadata_verbs_answer_unchanged_for_an_anonymous_caller()
    {
        // The metadata and existence verbs became gated in #1721. Under the
        // default null gate they must still answer exactly as they did before,
        // for an anonymous caller with no policy: enforcement short-circuits
        // before the subject is ever resolved.
        const string tree = "zerocost-metadata";
        var lattice = Lattice(tree);

        Assert.That(await lattice.TreeExistsAsync(), Is.False,
            "an unwritten tree is absent, gate or no gate");

        await lattice.SetAsync("a:1", Bytes("1"));
        await lattice.SetAsync("a:2", Bytes("2"));

        var report = await lattice.DiagnoseAsync();
        var usage = await lattice.GetStorageUsageAsync();
        var retention = await lattice.GetHistoryRetentionAsync();
        var exists = await lattice.TreeExistsAsync();

        Assert.Multiple(() =>
        {
            Assert.That(exists, Is.True);
            Assert.That(report.TotalLiveKeys, Is.EqualTo(2));
            Assert.That(usage.TreeId, Is.EqualTo(tree));
            Assert.That(usage.LiveKeys, Is.EqualTo(2));
            Assert.That(retention.Mode, Is.EqualTo(HistoryRetentionMode.MetadataOnly));
        });
    }

    [Test]
    public async Task With_no_auth_registered_the_lifecycle_and_projection_verbs_answer_unchanged()
    {
        // The lifecycle-status and projection verbs became gated in #1733, and
        // GetOrSetAsync gained a second (Read) enforcement call. Under the default
        // null gate all of them must still answer exactly as they did before, for
        // an anonymous caller with no policy: every enforcement short-circuits on
        // the null gate before the subject is ever resolved.
        const string tree = "zerocost-lifecycle";
        var lattice = Lattice(tree);

        await lattice.SetAsync("a:1", Bytes("1"));

        var merge = await lattice.IsMergeCompleteAsync();
        var snapshot = await lattice.IsSnapshotCompleteAsync();
        var resize = await lattice.IsResizeCompleteAsync();
        var reshard = await lattice.IsReshardCompleteAsync();
        var lag = await lattice.GetMaterialiserLagAsync();

        // GetOrSet still round-trips both legs: insert returns null, and the
        // read-hit path still discloses the stored value with no gate in the way.
        var inserted = await lattice.GetOrSetAsync("a:2", Bytes("first"));
        var hit = await lattice.GetOrSetAsync("a:2", Bytes("second"));

        Assert.Multiple(() =>
        {
            Assert.That(merge, Is.True);
            Assert.That(snapshot, Is.True);
            Assert.That(resize, Is.True);
            Assert.That(reshard, Is.True);
            Assert.That(lag, Is.GreaterThanOrEqualTo(0));
            Assert.That(inserted, Is.Null);
            Assert.That(Encoding.UTF8.GetString(hit!), Is.EqualTo("first"));
        });
    }
}

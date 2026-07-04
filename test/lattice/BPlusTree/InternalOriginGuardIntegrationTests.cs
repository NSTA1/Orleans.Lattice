using System.Text;
using Orleans.Lattice;
using Orleans.Lattice.BPlusTree;
using Orleans.Runtime;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Regression coverage for the defense-in-depth internal-origin assertion on the
/// physical shard and leaf grains, and for the capability-stripping incoming call
/// filter that backs it (issue #1103). All access-gate enforcement lives on the
/// <see cref="ILattice"/> facade; the shard and leaf grains it delegates to
/// enforce no policy of their own. A direct in-cluster Orleans grain call to a
/// shard or leaf key would therefore bypass policy. These tests prove that such a
/// direct external call is refused, that the same operation through the facade
/// still succeeds, and that a client cannot smuggle a forged internal-origin (or
/// system-origin) marker past the filter to defeat the guard.
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed partial class InternalOriginGuardIntegrationTests
{
    private InternalOriginGuardClusterFixture _fixture = null!;
    private TestCluster _cluster = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new InternalOriginGuardClusterFixture();
        await _fixture.InitializeAsync();
        _cluster = _fixture.Cluster;
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown() => await _fixture.DisposeAsync();

    [TearDown]
    public void TearDown() => RequestContext.Clear();

    private static byte[] Val(string key) => Encoding.UTF8.GetBytes(key);

    // A direct external client call to the shard root grain is client-sourced, so
    // the filter strips any capability keys and stamps no internal-origin marker;
    // the guard must refuse it.

    [Test]
    public void ApplyCrdtDeltaAsync_direct_external_call_without_internal_origin_is_refused()
    {
        var shard = _cluster.GrainFactory.GetGrain<IShardRootGrain>("origin-guard-crdt/0");

        Assert.ThrowsAsync<LatticeAuthorizationDeniedException>(
            async () => await shard.ApplyCrdtDeltaAsync("k", LatticeMergeMode.LwwRegister, Val("k")));
    }

    [Test]
    public void BulkLoadAsync_direct_external_call_without_internal_origin_is_refused()
    {
        var shard = _cluster.GrainFactory.GetGrain<IShardRootGrain>("origin-guard-bulk/0");
        var entries = new List<KeyValuePair<string, byte[]>> { new("k", Val("k")) };

        Assert.ThrowsAsync<LatticeAuthorizationDeniedException>(
            async () => await shard.BulkLoadAsync("op-1", entries));
    }

    [Test]
    public void Leaf_mutation_direct_external_call_without_internal_origin_is_refused()
    {
        var leaf = _cluster.GrainFactory.GetGrain<IBPlusLeafGrain>(Guid.NewGuid());

        Assert.ThrowsAsync<LatticeAuthorizationDeniedException>(
            async () => await leaf.GetOrSetAsync("k", Val("k")));
    }

    // The same write through the facade succeeds: the facade-to-shard hop is
    // silo-sourced, so the filter stamps the internal-origin marker and the guard
    // passes. This proves the guard does not break the legitimate call graph.

    [Test]
    public async Task Facade_write_that_delegates_to_the_shard_still_succeeds()
    {
        var tree = _cluster.GrainFactory.GetGrain<ILattice>("origin-guard-facade");

        await tree.SetAsync("k", Val("k"));
        var read = await tree.GetAsync("k");

        Assert.That(read, Is.EqualTo(Val("k")));
    }

    // A malicious client that manually seeds the internal-origin marker on the
    // inbound RequestContext must not defeat the guard: the filter strips the
    // forged key before the grain body runs, so the guard still refuses.

    [Test]
    public void Forged_internal_origin_marker_from_a_client_is_stripped_and_still_refused()
    {
        var shard = _cluster.GrainFactory.GetGrain<IShardRootGrain>("origin-guard-forged-igo/0");
        RequestContext.Set(LatticeEventConstants.InternalGrainOriginRequestContextKey, true);

        Assert.ThrowsAsync<LatticeAuthorizationDeniedException>(
            async () => await shard.ApplyCrdtDeltaAsync("k", LatticeMergeMode.LwwRegister, Val("k")));
    }

    [Test]
    public void Forged_system_origin_marker_from_a_client_is_stripped_and_still_refused()
    {
        var shard = _cluster.GrainFactory.GetGrain<IShardRootGrain>("origin-guard-forged-sysorig/0");
        RequestContext.Set(LatticeEventConstants.AccessGateSystemOriginRequestContextKey, true);

        Assert.ThrowsAsync<LatticeAuthorizationDeniedException>(
            async () => await shard.ApplyCrdtDeltaAsync("k", LatticeMergeMode.LwwRegister, Val("k")));
    }
}

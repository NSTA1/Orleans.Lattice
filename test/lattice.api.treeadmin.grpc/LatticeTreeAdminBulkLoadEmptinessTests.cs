namespace Orleans.Lattice.Api.TreeAdmin.Grpc.Tests;

/// <summary>
/// Real-cluster regression coverage for the bulk-load emptiness precondition on
/// <see cref="ILatticeTreeAdmin.BeginBulkLoadAsync"/>. The facade's own unit tests
/// stub the diagnostic with a substitute that fabricates a tombstone count
/// regardless of the <c>deep</c> argument, so they pass over a guard that was dead
/// in production: the shallow diagnostic path never populates
/// <c>TotalTombstones</c>, so a tree emptied by deleting every key reported both
/// counts as zero and wrongly passed the emptiness probe. These tests drive a
/// genuinely emptied tree through a live silo, which is the only way to exercise
/// the real diagnostic path the facade probes.
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class LatticeTreeAdminBulkLoadEmptinessTests
{
    private const string Op = "bulk-load-op";

    private GrpcTreeAdminClusterFixture _fixture = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new GrpcTreeAdminClusterFixture();
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

    [Test]
    public async Task BeginBulkLoadAsync_rejects_a_tree_emptied_by_deletion()
    {
        const string tree = "emptied-by-deletion";
        var grain = _fixture.GrainFactory.GetGrain<ILattice>(tree);

        await grain.SetAsync("a", "{}"u8.ToArray());
        await grain.SetAsync("b", "{}"u8.ToArray());
        Assert.That(await grain.DeleteAsync("a"), Is.True);
        Assert.That(await grain.DeleteAsync("b"), Is.True);

        // The tree now holds only tombstones: no live keys, but it is not a fresh
        // tree. The real deep diagnostic sees the tombstones, so the precondition
        // must reject it rather than graft onto tombstoned leaves.
        var report = await grain.DiagnoseAsync(deep: true);
        Assert.That(report.TotalLiveKeys, Is.Zero, "sanity: the tree has no live keys");
        Assert.That(report.TotalTombstones, Is.GreaterThan(0), "sanity: the tree carries tombstones");

        Assert.That(
            async () => await _fixture.Control.BeginBulkLoadAsync(tree, Op),
            Throws.TypeOf<TreeNotEmptyException>());
    }

    [Test]
    public async Task BeginBulkLoadAsync_admits_a_genuinely_empty_tree()
    {
        const string tree = "never-written";

        var session = await _fixture.Control.BeginBulkLoadAsync(tree, Op);

        Assert.Multiple(() =>
        {
            Assert.That(session.TreeId, Is.EqualTo(tree));
            Assert.That(session.OperationId, Is.EqualTo(Op));
        });
    }
}

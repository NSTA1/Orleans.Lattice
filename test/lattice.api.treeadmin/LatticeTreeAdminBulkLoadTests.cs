using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice;
using Orleans.Lattice.Api.Data;
using Orleans.Lattice.Api.Schema;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Api.TreeAdmin.Tests;

/// <summary>
/// Unit tests for the streamed, resumable, idempotency-keyed bulk-load (tree
/// creation) protocol on <see cref="LatticeTreeAdmin"/>: the begin / append /
/// commit verbs. Each authorizes the whole-tree <c>BulkLoad</c> capability
/// fail-closed, then delegates to the public <see cref="ILattice"/> grain -
/// begin and commit read the tree's diagnostic snapshot, append validates the
/// chunk is strictly ascending and grafts it under a per-chunk operation id.
/// Driven purely with substitutes and a hand-written access gate - no cluster.
/// </summary>
[TestFixture]
public sealed class LatticeTreeAdminBulkLoadTests
{
    private const string Tree = "orders";
    private const string Op = "load-2024";

    private sealed class FixedGate : ILatticeAccessGate
    {
        private readonly bool _allow;
        public FixedGate(bool allow) => _allow = allow;

        public ValueTask<LatticeAccessDecision> AuthorizeAsync(
            in LatticeAccessRequest request, CancellationToken cancellationToken = default)
            => new(_allow ? LatticeAccessDecision.Allow() : LatticeAccessDecision.Deny("denied by test"));
    }

    private static LatticeTreeAdmin Create(IGrainFactory factory, bool allow = true)
        => new(
            Substitute.For<ILatticeSchemaControl>(),
            factory,
            new TreeAdminAccessAuthorizer(new FixedGate(allow)),
            Options.Create(new LatticeApiTreeAdminOptions()));

    private static ILattice Wire(IGrainFactory factory)
    {
        var lattice = Substitute.For<ILattice>();
        factory.GetGrain<ILattice>(Tree).Returns(lattice);
        return lattice;
    }

    private static void StubDiagnose(ILattice lattice, long liveKeys, long tombstones = 0)
        => lattice.DiagnoseAsync(Arg.Any<bool>(), Arg.Any<CancellationToken>())
            .Returns(new TreeDiagnosticReport { TotalLiveKeys = liveKeys, TotalTombstones = tombstones });

    private static IReadOnlyList<DataEntry> Entries(params string[] keys)
        => keys.Select(k => new DataEntry { Key = k, Value = [1] }).ToArray();

    // ----- BeginBulkLoad -----

    [Test]
    public async Task BeginBulkLoadAsync_on_empty_tree_returns_the_session()
    {
        var factory = Substitute.For<IGrainFactory>();
        var lattice = Wire(factory);
        StubDiagnose(lattice, liveKeys: 0);
        var facade = Create(factory);

        var session = await facade.BeginBulkLoadAsync(Tree, Op);

        Assert.Multiple(() =>
        {
            Assert.That(session.TreeId, Is.EqualTo(Tree));
            Assert.That(session.OperationId, Is.EqualTo(Op));
        });
    }

    [Test]
    public void BeginBulkLoadAsync_on_tree_with_live_keys_throws_TreeNotEmpty()
    {
        var factory = Substitute.For<IGrainFactory>();
        var lattice = Wire(factory);
        StubDiagnose(lattice, liveKeys: 3);
        var facade = Create(factory);

        Assert.That(async () => await facade.BeginBulkLoadAsync(Tree, Op),
            Throws.TypeOf<TreeNotEmptyException>());
    }

    [Test]
    public void BeginBulkLoadAsync_on_tree_with_tombstones_throws_TreeNotEmpty()
    {
        var factory = Substitute.For<IGrainFactory>();
        var lattice = Wire(factory);
        StubDiagnose(lattice, liveKeys: 0, tombstones: 2);
        var facade = Create(factory);

        Assert.That(async () => await facade.BeginBulkLoadAsync(Tree, Op),
            Throws.TypeOf<TreeNotEmptyException>());
    }

    [Test]
    public void BeginBulkLoadAsync_denied_by_gate_throws_and_does_not_dial_lattice()
    {
        var factory = Substitute.For<IGrainFactory>();
        var lattice = Wire(factory);
        var facade = Create(factory, allow: false);

        Assert.That(async () => await facade.BeginBulkLoadAsync(Tree, Op),
            Throws.TypeOf<LatticeAuthorizationDeniedException>());
        lattice.DidNotReceive().DiagnoseAsync(Arg.Any<bool>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public void BeginBulkLoadAsync_reserved_tree_id_is_rejected()
    {
        var facade = Create(Substitute.For<IGrainFactory>());

        Assert.That(async () => await facade.BeginBulkLoadAsync(LatticeConstants.SystemTreePrefix + "trees", Op),
            Throws.ArgumentException);
    }

    [Test]
    public void BeginBulkLoadAsync_invalid_operation_id_is_rejected()
    {
        var facade = Create(Substitute.For<IGrainFactory>());

        Assert.Multiple(() =>
        {
            Assert.That(async () => await facade.BeginBulkLoadAsync(Tree, null!), Throws.InstanceOf<ArgumentException>());
            Assert.That(async () => await facade.BeginBulkLoadAsync(Tree, ""), Throws.ArgumentException);
            Assert.That(async () => await facade.BeginBulkLoadAsync(Tree, "a/b"), Throws.ArgumentException);
        });
    }

    [Test]
    public void BeginBulkLoadAsync_null_or_empty_tree_id_throws()
    {
        var facade = Create(Substitute.For<IGrainFactory>());

        Assert.Multiple(() =>
        {
            Assert.That(async () => await facade.BeginBulkLoadAsync(null!, Op), Throws.TypeOf<ArgumentNullException>());
            Assert.That(async () => await facade.BeginBulkLoadAsync("", Op), Throws.ArgumentException);
        });
    }

    // ----- AppendBulkLoad -----

    [Test]
    public async Task AppendBulkLoadAsync_grafts_the_chunk_under_a_per_chunk_operation_id()
    {
        var factory = Substitute.For<IGrainFactory>();
        var lattice = Wire(factory);
        lattice.BulkAppendChunkAsync(
                Arg.Any<string>(),
                Arg.Any<IReadOnlyList<KeyValuePair<string, byte[]>>>(),
                Arg.Any<CancellationToken>())
            .Returns(3);
        var facade = Create(factory);

        var ack = await facade.AppendBulkLoadAsync(Tree, Op, chunkIndex: 7, Entries("a", "b", "c"));

        Assert.Multiple(() =>
        {
            Assert.That(ack.TreeId, Is.EqualTo(Tree));
            Assert.That(ack.OperationId, Is.EqualTo(Op));
            Assert.That(ack.ChunkIndex, Is.EqualTo(7));
            Assert.That(ack.AcceptedEntryCount, Is.EqualTo(3));
            Assert.That(ack.NextChunkIndex, Is.EqualTo(8));
        });
        await lattice.Received(1).BulkAppendChunkAsync(
            $"{Op}/7",
            Arg.Is<IReadOnlyList<KeyValuePair<string, byte[]>>>(p => p.Count == 3),
            Arg.Any<CancellationToken>());
    }

    [Test]
    public void AppendBulkLoadAsync_out_of_order_chunk_throws_and_does_not_dial_lattice()
    {
        var factory = Substitute.For<IGrainFactory>();
        var lattice = Wire(factory);
        var facade = Create(factory);

        Assert.That(async () => await facade.AppendBulkLoadAsync(Tree, Op, 0, Entries("a", "c", "b")),
            Throws.TypeOf<BulkLoadOrderException>());
        lattice.DidNotReceive().BulkAppendChunkAsync(
            Arg.Any<string>(),
            Arg.Any<IReadOnlyList<KeyValuePair<string, byte[]>>>(),
            Arg.Any<CancellationToken>());
    }

    [Test]
    public void AppendBulkLoadAsync_duplicate_key_within_chunk_throws()
    {
        var factory = Substitute.For<IGrainFactory>();
        Wire(factory);
        var facade = Create(factory);

        Assert.That(async () => await facade.AppendBulkLoadAsync(Tree, Op, 0, Entries("a", "a")),
            Throws.TypeOf<BulkLoadOrderException>());
    }

    [Test]
    public void AppendBulkLoadAsync_denied_by_gate_throws()
    {
        var factory = Substitute.For<IGrainFactory>();
        Wire(factory);
        var facade = Create(factory, allow: false);

        Assert.That(async () => await facade.AppendBulkLoadAsync(Tree, Op, 0, Entries("a")),
            Throws.TypeOf<LatticeAuthorizationDeniedException>());
    }

    [Test]
    public void AppendBulkLoadAsync_negative_chunk_index_throws()
    {
        var facade = Create(Substitute.For<IGrainFactory>());

        Assert.That(async () => await facade.AppendBulkLoadAsync(Tree, Op, -1, Entries("a")),
            Throws.TypeOf<ArgumentOutOfRangeException>());
    }

    [Test]
    public void AppendBulkLoadAsync_null_entries_throws()
    {
        var facade = Create(Substitute.For<IGrainFactory>());

        Assert.That(async () => await facade.AppendBulkLoadAsync(Tree, Op, 0, null!),
            Throws.TypeOf<ArgumentNullException>());
    }

    [Test]
    public void AppendBulkLoadAsync_invalid_operation_id_is_rejected()
    {
        var facade = Create(Substitute.For<IGrainFactory>());

        Assert.That(async () => await facade.AppendBulkLoadAsync(Tree, "a/b", 0, Entries("a")),
            Throws.ArgumentException);
    }

    [Test]
    public async Task AppendBulkLoadAsync_empty_chunk_is_accepted()
    {
        var factory = Substitute.For<IGrainFactory>();
        var lattice = Wire(factory);
        lattice.BulkAppendChunkAsync(
                Arg.Any<string>(),
                Arg.Any<IReadOnlyList<KeyValuePair<string, byte[]>>>(),
                Arg.Any<CancellationToken>())
            .Returns(0);
        var facade = Create(factory);

        var ack = await facade.AppendBulkLoadAsync(Tree, Op, 0, Entries());

        Assert.That(ack.AcceptedEntryCount, Is.EqualTo(0));
    }

    // ----- CommitBulkLoad -----

    [Test]
    public async Task CommitBulkLoadAsync_returns_the_observed_live_key_count()
    {
        var factory = Substitute.For<IGrainFactory>();
        var lattice = Wire(factory);
        lattice.CountAsync(Arg.Any<CancellationToken>()).Returns(42);
        var facade = Create(factory);

        var result = await facade.CommitBulkLoadAsync(Tree, Op);

        Assert.Multiple(() =>
        {
            Assert.That(result.TreeId, Is.EqualTo(Tree));
            Assert.That(result.OperationId, Is.EqualTo(Op));
            Assert.That(result.TotalLiveKeys, Is.EqualTo(42));
        });
    }

    [Test]
    public void CommitBulkLoadAsync_denied_by_gate_throws()
    {
        var factory = Substitute.For<IGrainFactory>();
        Wire(factory);
        var facade = Create(factory, allow: false);

        Assert.That(async () => await facade.CommitBulkLoadAsync(Tree, Op),
            Throws.TypeOf<LatticeAuthorizationDeniedException>());
    }
}

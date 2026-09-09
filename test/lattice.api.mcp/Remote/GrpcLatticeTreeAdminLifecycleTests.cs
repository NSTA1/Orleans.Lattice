using Orleans.Lattice.Api.Data;
using Orleans.Lattice.Api.TreeAdmin;
using Orleans.Lattice.Api.TreeAdmin.Grpc;

namespace Orleans.Lattice.Api.Mcp.Tests;

/// <summary>
/// Unit tests for the tree deletion-lifecycle and bulk-load block of
/// <see cref="GrpcLatticeTreeAdmin"/> - the seven members that
/// <see cref="GrpcLatticeTreeAdminTests"/> does not reach.
/// </summary>
/// <remarks>
/// These are the destructive and high-volume administration operations
/// (soft-delete, recover, purge, and the three-phase bulk load), so an adapter
/// that dropped an argument would either target the wrong tree or silently
/// discard a chunk. Each member is proven to wrap its scalar arguments into the
/// correct wire request record - in particular that <c>PurgeTreeAsync</c>
/// forwards the operator's <c>confirm</c> flag rather than defaulting it, and
/// that <c>AppendBulkLoadAsync</c> carries the chunk index and payload intact.
/// Deterministic over a <see cref="FakeCallInvoker"/>.
/// </remarks>
[TestFixture]
public sealed class GrpcLatticeTreeAdminLifecycleTests
{
    private static GrpcLatticeTreeAdmin Adapter(FakeCallInvoker invoker)
        => new(RemoteTestSupport.TreeAdminClient(invoker));

    private static TreeDeletionStatus Status(string tree = "orders", bool deleted = true) => new()
    {
        TreeId = tree,
        IsDeleted = deleted,
        CanRecover = deleted,
    };

    [Test]
    public async Task DeleteTreeAsync_wraps_tree_id_and_unwraps_status()
    {
        var invoker = new FakeCallInvoker(_ => Status());

        var result = await Adapter(invoker).DeleteTreeAsync("orders");

        Assert.Multiple(() =>
        {
            Assert.That(((TreeAdminTreeRequest)invoker.LastRequest!).TreeId, Is.EqualTo("orders"));
            Assert.That(result.TreeId, Is.EqualTo("orders"));
            Assert.That(result.IsDeleted, Is.True);
        });
    }

    [Test]
    public async Task RecoverTreeAsync_wraps_tree_id_and_unwraps_status()
    {
        var invoker = new FakeCallInvoker(_ => Status(deleted: false));

        var result = await Adapter(invoker).RecoverTreeAsync("orders");

        Assert.Multiple(() =>
        {
            Assert.That(((TreeAdminTreeRequest)invoker.LastRequest!).TreeId, Is.EqualTo("orders"));
            Assert.That(result.IsDeleted, Is.False);
        });
    }

    [TestCase(true)]
    [TestCase(false)]
    public async Task PurgeTreeAsync_forwards_the_confirm_flag(bool confirm)
    {
        var invoker = new FakeCallInvoker(_ => Status() with { PurgeInProgress = confirm });

        var result = await Adapter(invoker).PurgeTreeAsync("orders", confirm);

        var sent = (TreeAdminPurgeRequest)invoker.LastRequest!;
        Assert.Multiple(() =>
        {
            Assert.That(sent.TreeId, Is.EqualTo("orders"));
            Assert.That(sent.Confirm, Is.EqualTo(confirm),
                "the operator's confirmation must not be defaulted by the adapter");
            Assert.That(result.PurgeInProgress, Is.EqualTo(confirm));
        });
    }

    [Test]
    public async Task GetTreeDeletionStatusAsync_wraps_tree_id_and_unwraps_status()
    {
        var invoker = new FakeCallInvoker(_ => Status() with { PurgeComplete = true });

        var result = await Adapter(invoker).GetTreeDeletionStatusAsync("orders");

        Assert.Multiple(() =>
        {
            Assert.That(((TreeAdminTreeRequest)invoker.LastRequest!).TreeId, Is.EqualTo("orders"));
            Assert.That(result.PurgeComplete, Is.True);
        });
    }

    [Test]
    public async Task BeginBulkLoadAsync_wraps_tree_and_operation_ids()
    {
        var invoker = new FakeCallInvoker(_ => new TreeBulkLoadSession { TreeId = "orders", OperationId = "op-1" });

        var result = await Adapter(invoker).BeginBulkLoadAsync("orders", "op-1");

        var sent = (TreeAdminBulkLoadSessionRequest)invoker.LastRequest!;
        Assert.Multiple(() =>
        {
            Assert.That(sent.TreeId, Is.EqualTo("orders"));
            Assert.That(sent.OperationId, Is.EqualTo("op-1"));
            Assert.That(result.OperationId, Is.EqualTo("op-1"));
        });
    }

    [Test]
    public async Task AppendBulkLoadAsync_carries_chunk_index_and_entries()
    {
        var invoker = new FakeCallInvoker(_ => new TreeBulkLoadChunkAck
        {
            TreeId = "orders",
            OperationId = "op-1",
            ChunkIndex = 7,
            AcceptedEntryCount = 2,
            NextChunkIndex = 8,
        });
        var entries = new[]
        {
            new DataEntry { Key = "a", Value = new byte[] { 1 } },
            new DataEntry { Key = "b", Value = new byte[] { 2 } },
        };

        var result = await Adapter(invoker).AppendBulkLoadAsync("orders", "op-1", 7, entries);

        var sent = (TreeAdminBulkLoadAppendRequest)invoker.LastRequest!;
        Assert.Multiple(() =>
        {
            Assert.That(sent.TreeId, Is.EqualTo("orders"));
            Assert.That(sent.OperationId, Is.EqualTo("op-1"));
            Assert.That(sent.ChunkIndex, Is.EqualTo(7), "a dropped chunk index would silently reorder the load");
            Assert.That(sent.Entries, Is.EqualTo(entries));
            Assert.That(result.NextChunkIndex, Is.EqualTo(8));
        });
    }

    [Test]
    public async Task CommitBulkLoadAsync_wraps_tree_and_operation_ids()
    {
        var invoker = new FakeCallInvoker(_ => new TreeBulkLoadResult
        {
            TreeId = "orders",
            OperationId = "op-1",
            TotalLiveKeys = 42,
        });

        var result = await Adapter(invoker).CommitBulkLoadAsync("orders", "op-1");

        var sent = (TreeAdminBulkLoadSessionRequest)invoker.LastRequest!;
        Assert.Multiple(() =>
        {
            Assert.That(sent.TreeId, Is.EqualTo("orders"));
            Assert.That(sent.OperationId, Is.EqualTo("op-1"));
            Assert.That(result.TotalLiveKeys, Is.EqualTo(42));
        });
    }

    [Test]
    public void Lifecycle_members_reject_an_empty_tree_id()
    {
        var adapter = Adapter(new FakeCallInvoker(_ => Status()));

        Assert.Multiple(() =>
        {
            Assert.That(async () => await adapter.DeleteTreeAsync(""), Throws.ArgumentException);
            Assert.That(async () => await adapter.RecoverTreeAsync(""), Throws.ArgumentException);
            Assert.That(async () => await adapter.PurgeTreeAsync("", confirm: true), Throws.ArgumentException);
            Assert.That(async () => await adapter.GetTreeDeletionStatusAsync(""), Throws.ArgumentException);
            Assert.That(async () => await adapter.BeginBulkLoadAsync("", "op-1"), Throws.ArgumentException);
            Assert.That(async () => await adapter.CommitBulkLoadAsync("", "op-1"), Throws.ArgumentException);
            Assert.That(
                async () => await adapter.AppendBulkLoadAsync("", "op-1", 0, Array.Empty<DataEntry>()),
                Throws.ArgumentException);
        });
    }
}

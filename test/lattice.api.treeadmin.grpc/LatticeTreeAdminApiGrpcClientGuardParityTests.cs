using Grpc.Core;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Api.Data;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.TreeAdmin.Grpc.Tests;

/// <summary>
/// Pins the client-side argument guards that mirror the facade's own checks on
/// the bulk-load and restore operations. Without them the client sent requests the
/// server could only reject, so a remote caller saw an <see cref="RpcException"/>
/// after a round trip where a local caller of the same contract saw an
/// <see cref="ArgumentException"/> - and the restore operation id's documented
/// "must not be empty when supplied" rule was not enforced at all.
/// </summary>
[TestFixture]
public sealed class LatticeTreeAdminApiGrpcClientGuardParityTests
{
    private ServiceProvider _services = null!;
    private LatticeTreeAdminGrpcMethods _methods = null!;

    [OneTimeSetUp]
    public void OneTimeSetUp()
    {
        _services = new ServiceCollection().AddSerializer().BuildServiceProvider();
        _methods = LatticeTreeAdminGrpcMethods.FromServiceProvider(_services);
    }

    [OneTimeTearDown]
    public void OneTimeTearDown() => _services.Dispose();

    private (LatticeTreeAdminApiGrpcClient Client, UnaryResponseCallInvoker Invoker) Create(object response)
    {
        var invoker = new UnaryResponseCallInvoker(response);
        return (new LatticeTreeAdminApiGrpcClient(invoker, _methods), invoker);
    }

    private static IReadOnlyList<DataEntry> Chunk(params string[] keys) =>
        keys.Select(k => new DataEntry { Key = k, Value = "{}"u8.ToArray() }).ToArray();

    private static TreeBulkLoadSession Session() => new() { TreeId = "orders", OperationId = "op-1" };

    private static TreeBulkLoadChunkAck Ack() => new()
    {
        TreeId = "orders",
        OperationId = "op-1",
        ChunkIndex = 0,
        AcceptedEntryCount = 1,
        NextChunkIndex = 1,
    };

    private static TreeBulkLoadResult Committed() => new() { TreeId = "orders", OperationId = "op-1", TotalLiveKeys = 1 };

    private static TreeRestoreResult Restored() => new()
    {
        BackupId = "bk-1",
        TargetTreeId = "orders",
        Mode = TreeRestoreMode.ShadowCutover,
        OperationId = "op-1",
        ManifestChain = ["m-1"],
        EntriesApplied = 3,
    };

    [Test]
    public void BeginBulkLoadAsync_rejects_an_operation_id_containing_a_slash_without_sending()
    {
        var (client, invoker) = Create(Session());

        var ex = Assert.ThrowsAsync<ArgumentException>(async () =>
            await client.BeginBulkLoadAsync("orders", "op/1"));

        Assert.Multiple(() =>
        {
            Assert.That(ex!.ParamName, Is.EqualTo("operationId"));
            Assert.That(invoker.LastRequest, Is.Null, "A request the server must reject was sent.");
        });
    }

    [Test]
    public void AppendBulkLoadAsync_rejects_an_operation_id_containing_a_slash_without_sending()
    {
        var (client, invoker) = Create(Ack());

        var ex = Assert.ThrowsAsync<ArgumentException>(async () =>
            await client.AppendBulkLoadAsync("orders", "op/1", 0, Chunk("a")));

        Assert.Multiple(() =>
        {
            Assert.That(ex!.ParamName, Is.EqualTo("operationId"));
            Assert.That(invoker.LastRequest, Is.Null, "A request the server must reject was sent.");
        });
    }

    [Test]
    public void AppendBulkLoadAsync_rejects_a_negative_chunk_index_without_sending()
    {
        var (client, invoker) = Create(Ack());

        var ex = Assert.ThrowsAsync<ArgumentOutOfRangeException>(async () =>
            await client.AppendBulkLoadAsync("orders", "op-1", -1, Chunk("a")));

        Assert.Multiple(() =>
        {
            Assert.That(ex!.ParamName, Is.EqualTo("chunkIndex"));
            Assert.That(invoker.LastRequest, Is.Null, "A request the server must reject was sent.");
        });
    }

    [Test]
    public void CommitBulkLoadAsync_rejects_an_operation_id_containing_a_slash_without_sending()
    {
        var (client, invoker) = Create(Committed());

        var ex = Assert.ThrowsAsync<ArgumentException>(async () =>
            await client.CommitBulkLoadAsync("orders", "op/1"));

        Assert.Multiple(() =>
        {
            Assert.That(ex!.ParamName, Is.EqualTo("operationId"));
            Assert.That(invoker.LastRequest, Is.Null, "A request the server must reject was sent.");
        });
    }

    [Test]
    public async Task The_bulk_load_operations_send_a_valid_operation_id_and_chunk_index()
    {
        var (beginClient, beginInvoker) = Create(Session());
        var (appendClient, appendInvoker) = Create(Ack());
        var (commitClient, commitInvoker) = Create(Committed());

        await beginClient.BeginBulkLoadAsync("orders", "op-1");
        await appendClient.AppendBulkLoadAsync("orders", "op-1", 0, Chunk("a"));
        await commitClient.CommitBulkLoadAsync("orders", "op-1");

        Assert.Multiple(() =>
        {
            Assert.That(((TreeAdminBulkLoadSessionRequest)beginInvoker.LastRequest!).OperationId, Is.EqualTo("op-1"));
            Assert.That(((TreeAdminBulkLoadAppendRequest)appendInvoker.LastRequest!).ChunkIndex, Is.Zero);
            Assert.That(((TreeAdminBulkLoadSessionRequest)commitInvoker.LastRequest!).OperationId, Is.EqualTo("op-1"));
        });
    }

    [Test]
    public void RestoreTreeAsync_rejects_a_supplied_but_empty_operation_id_without_sending()
    {
        var (client, invoker) = Create(Restored());

        var ex = Assert.ThrowsAsync<ArgumentException>(async () =>
            await client.RestoreTreeAsync("orders", "bk-1", operationId: string.Empty));

        Assert.Multiple(() =>
        {
            Assert.That(ex!.ParamName, Is.EqualTo("operationId"));
            Assert.That(invoker.LastRequest, Is.Null, "A request the server must reject was sent.");
        });
    }

    [Test]
    public async Task RestoreTreeAsync_sends_an_omitted_operation_id_as_null_for_the_server_to_derive()
    {
        var (client, invoker) = Create(Restored());

        await client.RestoreTreeAsync("orders", "bk-1");

        var request = (TreeAdminRestoreRequest)invoker.LastRequest!;
        Assert.Multiple(() =>
        {
            Assert.That(request.OperationId, Is.Null);
            Assert.That(request.BackupId, Is.EqualTo("bk-1"));
        });
    }
}

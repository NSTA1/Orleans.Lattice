using Grpc.Core;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.State.Grpc.Tests;

/// <summary>
/// Unit coverage for the concrete (non-null <c>serviceImpl</c>) arm of
/// <see cref="LatticeStateGrpcServiceBase.BindService"/>. The gRPC binder
/// invokes <c>BindService</c> twice: once with a <see langword="null"/>
/// instance to record metadata (covered elsewhere) and once with the resolved
/// service to attach the per-RPC handlers. This fixture drives the second arm
/// in-process against a recording <see cref="ServiceBinderBase"/> double, so no
/// Orleans cluster or gRPC server is stood up.
/// </summary>
[TestFixture]
public sealed class LatticeStateGrpcServiceBaseBindServiceTests
{
    private ServiceProvider _services = null!;

    [OneTimeSetUp]
    public void OneTimeSetUp() =>
        _services = new ServiceCollection().AddSerializer().BuildServiceProvider();

    [OneTimeTearDown]
    public void OneTimeTearDown() => _services.Dispose();

    [Test]
    public void BindService_with_concrete_implementation_registers_every_rpc_handler()
    {
        LatticeStateGrpcMethodsHolder.Current = LatticeStateGrpcMethods.FromServiceProvider(_services);
        var binder = new RecordingServiceBinder();
        var service = new StubStateGrpcService();

        LatticeStateGrpcServiceBase.BindService(binder, service);

        Assert.Multiple(() =>
        {
            // 17 unary RPCs plus the 2 server-streaming subscription RPCs.
            Assert.That(binder.UnaryCount, Is.EqualTo(17));
            Assert.That(binder.ServerStreamingCount, Is.EqualTo(2));
        });
    }

    private sealed class RecordingServiceBinder : ServiceBinderBase
    {
        public int UnaryCount { get; private set; }

        public int ServerStreamingCount { get; private set; }

        public override void AddMethod<TRequest, TResponse>(
            Method<TRequest, TResponse> method,
            UnaryServerMethod<TRequest, TResponse>? handler)
        {
            Assert.That(handler, Is.Not.Null);
            UnaryCount++;
        }

        public override void AddMethod<TRequest, TResponse>(
            Method<TRequest, TResponse> method,
            ServerStreamingServerMethod<TRequest, TResponse>? handler)
        {
            Assert.That(handler, Is.Not.Null);
            ServerStreamingCount++;
        }
    }

    private sealed class StubStateGrpcService : LatticeStateGrpcServiceBase
    {
        public override Task<TreeCatalogPage> ListTrees(CatalogRequest request, ServerCallContext context)
            => throw new NotSupportedException();

        public override Task<ViewCatalogPage> ListViews(CatalogRequest request, ServerCallContext context)
            => throw new NotSupportedException();

        public override Task<TagIndexCatalogPage> ListTagIndexes(CatalogRequest request, ServerCallContext context)
            => throw new NotSupportedException();

        public override Task<TagValueCatalogPage> ListTagValues(CatalogRequest request, ServerCallContext context)
            => throw new NotSupportedException();

        public override Task<CoveredTreeCatalogPage> ListCoveredTrees(CatalogRequest request, ServerCallContext context)
            => throw new NotSupportedException();

        public override Task<TagValueCatalogPage> ListIndexTags(CatalogRequest request, ServerCallContext context)
            => throw new NotSupportedException();

        public override Task<TagMemberScanPage> ScanTagMembers(TagMemberScanRequest request, ServerCallContext context)
            => throw new NotSupportedException();

        public override Task<StructureResponse> GetTreeStructure(StructureRequest request, ServerCallContext context)
            => throw new NotSupportedException();

        public override Task<EntryScanResponse> ScanEntries(EntryScanRequest request, ServerCallContext context)
            => throw new NotSupportedException();

        public override Task<EntryGetResponse> GetEntry(EntryGetRequest request, ServerCallContext context)
            => throw new NotSupportedException();

        public override Task<EntryHistoryResponse> GetEntryHistory(EntryHistoryRequest request, ServerCallContext context)
            => throw new NotSupportedException();

        public override Task<EntryScanCancelResponse> CancelScan(EntryScanCancelRequest request, ServerCallContext context)
            => throw new NotSupportedException();

        public override Task ObserveChanges(
            StateObserveRequest request,
            IServerStreamWriter<StateChangeNotification> responseStream,
            ServerCallContext context)
            => throw new NotSupportedException();

        public override Task ObserveMetrics(
            TreeMetricsRequest request,
            IServerStreamWriter<TreeMetricsSnapshot> responseStream,
            ServerCallContext context)
            => throw new NotSupportedException();

        public override Task<TreeMetricsSnapshot> GetMetricsSnapshot(TreeMetricsRequest request, ServerCallContext context)
            => throw new NotSupportedException();

        public override Task<ClusterInfo> GetClusterInfo(ClusterInfoRequest request, ServerCallContext context)
            => throw new NotSupportedException();

        public override Task<AuthSchemeAdvertisement> GetAuthScheme(AuthSchemeAdvertisementRequest request, ServerCallContext context)
            => throw new NotSupportedException();

        public override Task<DeadLetterCountResponse> GetDeadLetterCount(DeadLetterCountRequest request, ServerCallContext context)
            => throw new NotSupportedException();

        public override Task<DeadLetterQueuePage> ListDeadLetters(DeadLetterQueueRequest request, ServerCallContext context)
            => throw new NotSupportedException();
    }
}

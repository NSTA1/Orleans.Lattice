using Grpc.Core;
using Orleans.Lattice.Api.State;
using Orleans.Lattice.Api.State.Grpc;
using Orleans.Lattice.Explorer.Core.Connection;

namespace Orleans.Lattice.Explorer.Tests.Connection;

public partial class LatticeStateConnectionTests
{
    [Test]
    public async Task VerifyCredential_AcceptedCall_ReportsAccepted_OnItsOwnChannel()
    {
        var shared = new FakeStateClient();
        var check = new FakeStateClient();
        var built = new List<LatticeConnectionSettings>();
        var (connection, _) = NewConnection(settings =>
        {
            built.Add(settings);
            return built.Count == 1 ? shared : check;
        });
        await connection.ConfigureAsync(Settings());
        var before = connection.Status;

        var result = await connection.VerifyCredentialAsync(Settings() with
        {
            Authentication = LatticeCallAuthentication.Basic("alice", "right"),
        });

        Assert.Multiple(() =>
        {
            Assert.That(result, Is.EqualTo(LatticeCredentialCheck.Accepted));
            Assert.That(built, Has.Count.EqualTo(2), "the check builds a channel of its own");
            Assert.That(built[1].Authentication, Is.Not.Null);
            Assert.That(check.DisposeCount, Is.EqualTo(1), "the check's channel is released");
            Assert.That(shared.DisposeCount, Is.Zero, "the shared channel is untouched");
            Assert.That(connection.Status, Is.EqualTo(before), "the shared status is untouched");
        });
    }

    [TestCase(StatusCode.Unauthenticated, LatticeCredentialCheck.Refused)]
    [TestCase(StatusCode.PermissionDenied, LatticeCredentialCheck.Accepted)]
    [TestCase(StatusCode.Unavailable, LatticeCredentialCheck.Unverified)]
    [TestCase(StatusCode.Internal, LatticeCredentialCheck.Unverified)]
    public async Task VerifyCredential_MapsTheEndpointsAnswer(StatusCode code, LatticeCredentialCheck expected)
    {
        var client = new FakeStateClient { ListTreesHandler = _ => throw new RpcException(new Status(code, "x")) };
        var (connection, _) = NewConnection(_ => client);

        var result = await connection.VerifyCredentialAsync(Settings() with
        {
            Authentication = LatticeCallAuthentication.Basic("alice", "wrong"),
        });

        Assert.That(result, Is.EqualTo(expected));
    }

    [Test]
    public async Task VerifyCredential_WithoutACredential_ReportsUnverified_WithoutACall()
    {
        var built = 0;
        var (connection, _) = NewConnection(_ =>
        {
            built++;
            return new FakeStateClient();
        });

        var result = await connection.VerifyCredentialAsync(Settings());

        Assert.Multiple(() =>
        {
            Assert.That(result, Is.EqualTo(LatticeCredentialCheck.Unverified));
            Assert.That(built, Is.Zero);
        });
    }

    [Test]
    public void VerifyCredential_NullSettings_Throws()
    {
        var (connection, _) = NewConnection(_ => new FakeStateClient());

        Assert.ThrowsAsync<ArgumentNullException>(() => connection.VerifyCredentialAsync(null!));
    }

    [Test]
    public async Task VerifyCredential_ADefaultImplementation_ReportsUnverified()
    {
        ILatticeStateConnection connection = new DefaultOnlyConnection();

        var result = await connection.VerifyCredentialAsync(Settings());

        Assert.That(result, Is.EqualTo(LatticeCredentialCheck.Unverified));
    }

    [Test]
    public async Task RefusedCredential_FaultsTheConnection_AsCredentialRefused()
    {
        var client = new FakeStateClient();
        var (connection, _) = NewConnection(_ => client);
        await connection.ConfigureAsync(Settings() with { Authentication = LatticeCallAuthentication.Basic("alice", "wrong") });
        client.ListTreesHandler = _ => throw Permanent();

        await connection.ProbeAsync();

        Assert.Multiple(() =>
        {
            Assert.That(connection.Status.State, Is.EqualTo(LatticeConnectionState.Faulted));
            Assert.That(connection.Status.RequiresAuthentication, Is.True);
            Assert.That(connection.Status.CredentialRefused, Is.True);
        });
    }

    [Test]
    public async Task UnauthenticatedAnonymousCall_AsksForSignIn_WithoutReportingARefusal()
    {
        var client = new FakeStateClient();
        var (connection, _) = NewConnection(_ => client);
        await connection.ConfigureAsync(Settings());
        client.ListTreesHandler = _ => throw Permanent();

        await connection.ProbeAsync();

        Assert.Multiple(() =>
        {
            Assert.That(connection.Status.RequiresAuthentication, Is.True);
            Assert.That(connection.Status.CredentialRefused, Is.False, "no credential was presented, so none was refused");
        });
    }

    [Test]
    public async Task ForbiddenSignedInCall_IsNotACredentialRefusal()
    {
        var client = new FakeStateClient();
        var (connection, _) = NewConnection(_ => client);
        await connection.ConfigureAsync(Settings() with { Authentication = LatticeCallAuthentication.Basic("dave", "right") });
        client.ListTreesHandler = _ => throw Denied();

        await connection.ProbeAsync();

        Assert.That(connection.Status.CredentialRefused, Is.False, "a caller without grants is a caller the cluster recognised");
    }

    /// <summary>A connection that leaves the credential check to the interface default.</summary>
    private sealed class DefaultOnlyConnection : ILatticeStateConnection
    {
        public LatticeConnectionStatus Status => LatticeConnectionStatus.Disconnected;

        public event Action<LatticeConnectionStatus>? StatusChanged { add { } remove { } }

        public Task ConfigureAsync(LatticeConnectionSettings settings, CancellationToken cancellationToken = default) => throw new NotSupportedException();

        public Task<bool> ReconnectAsync(CancellationToken cancellationToken = default) => throw new NotSupportedException();

        public Task<bool> ProbeAsync(CancellationToken cancellationToken = default) => throw new NotSupportedException();

        public Task<TreeCatalogPage> ListTreesAsync(CatalogRequest request, CancellationToken cancellationToken = default) => throw new NotSupportedException();

        public Task<ViewCatalogPage> ListViewsAsync(CatalogRequest request, CancellationToken cancellationToken = default) => throw new NotSupportedException();

        public Task<TagIndexCatalogPage> ListTagIndexesAsync(CatalogRequest request, CancellationToken cancellationToken = default) => throw new NotSupportedException();

        public Task<TagValueCatalogPage> ListTagValuesAsync(CatalogRequest request, CancellationToken cancellationToken = default) => throw new NotSupportedException();

        public Task<CoveredTreeCatalogPage> ListCoveredTreesAsync(CatalogRequest request, CancellationToken cancellationToken = default) => throw new NotSupportedException();

        public Task<TagValueCatalogPage> ListIndexTagsAsync(CatalogRequest request, CancellationToken cancellationToken = default) => throw new NotSupportedException();

        public Task<TagMemberScanPage> ScanTagMembersAsync(TagMemberScanRequest request, CancellationToken cancellationToken = default) => throw new NotSupportedException();

        public Task<StructureResponse> GetTreeStructureAsync(StructureRequest request, CancellationToken cancellationToken = default) => throw new NotSupportedException();

        public Task<EntryScanResponse> ScanEntriesAsync(EntryScanRequest request, CancellationToken cancellationToken = default) => throw new NotSupportedException();

        public Task<EntryGetResponse> GetEntryAsync(EntryGetRequest request, CancellationToken cancellationToken = default) => throw new NotSupportedException();

        public Task<EntryHistoryResponse> GetEntryHistoryAsync(EntryHistoryRequest request, CancellationToken cancellationToken = default) => throw new NotSupportedException();

        public Task<EntryScanCancelResponse> CancelScanAsync(EntryScanCancelRequest request, CancellationToken cancellationToken = default) => throw new NotSupportedException();

        public Task<TreeMetricsSnapshot> GetMetricsSnapshotAsync(TreeMetricsRequest request, CancellationToken cancellationToken = default) => throw new NotSupportedException();

        public Task<ClusterInfo> GetClusterInfoAsync(ClusterInfoRequest request, CancellationToken cancellationToken = default) => throw new NotSupportedException();

        public Task<DeadLetterCountResponse> GetDeadLetterCountAsync(DeadLetterCountRequest request, CancellationToken cancellationToken = default) => throw new NotSupportedException();

        public Task<DeadLetterQueuePage> ListDeadLettersAsync(DeadLetterQueueRequest request, CancellationToken cancellationToken = default) => throw new NotSupportedException();

        public IAsyncEnumerable<StateChangeNotification> ObserveChangesAsync(StateObserveRequest request, CancellationToken cancellationToken = default) => throw new NotSupportedException();

        public IAsyncEnumerable<TreeMetricsSnapshot> ObserveMetricsAsync(TreeMetricsRequest request, CancellationToken cancellationToken = default) => throw new NotSupportedException();

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }
}

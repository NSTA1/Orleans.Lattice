using Grpc.Core;
using Grpc.Net.Client;
using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Api.State.Grpc.Tests;

/// <summary>
/// End-to-end coverage for the state-API gRPC identity bridge and auth-backed
/// read visibility (issue #981). Drives the unary RPCs over an in-process
/// <c>TestServer</c> whose silo runs the enforcing
/// <see cref="ILatticeAccessGate"/>, and asserts that the caller's identity -
/// carried purely in the request's credential header - scopes what the wire
/// returns: a prefix-scoped reader sees only its permitted entries and trees,
/// an unauthorised key reads back as not-found, and a call with no resolvable
/// credential fails closed. This is the wire-level analogue of the in-process
/// visibility matrix, proving the <c>ServerCallContext</c> -&gt;
/// <c>LatticeCredentialContext</c> bridge fires around each read.
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class AuthGrpcStateVisibilityTests
{
    private const string TreeA = "grpc-vis-tree-a";
    private const string TreeB = "grpc-vis-tree-b";
    private const string Reader = "grpc-reader";

    private AuthGrpcStateClusterFixture _fixture = null!;
    private GrpcStateHost _host = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new AuthGrpcStateClusterFixture();
        await _fixture.InitializeAsync();

        await _fixture.CreatePopulatedTreeAsync(TreeA, "x/1", "x/2", "y/1");
        await _fixture.CreatePopulatedTreeAsync(TreeB, "b/1");

        // Reader may read only treeA under prefix "x/". No grant on treeB.
        await _fixture.GrantAsync(new LatticeAuthorizationRule(
            "grpc-reader-x-prefix",
            LatticeSubjectSelector.User(Reader),
            LatticeScope.Prefix(TreeA, "x/"),
            LatticeOperation.Read | LatticeOperation.RangeRead,
            LatticeEffect.Allow));

        _host = await _fixture.CreateGrpcHostAsync();
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown()
    {
        if (_host is not null)
        {
            await _host.DisposeAsync();
        }

        if (_fixture is not null)
        {
            await _fixture.DisposeAsync();
        }
    }

    private static CallOptions WithSubject(string? subject)
    {
        if (subject is null)
        {
            return new CallOptions();
        }

        var headers = new global::Grpc.Core.Metadata
        {
            { "authorization", $"{AuthGrpcStateClusterFixture.CredentialScheme} {subject}" },
        };
        return new CallOptions(headers);
    }

    private async Task<TResponse> CallAsync<TRequest, TResponse>(
        Method<TRequest, TResponse> method,
        TRequest request,
        string? subject)
        where TRequest : class
        where TResponse : class
    {
        var invoker = _host.Channel.CreateCallInvoker();
        using var call = invoker.AsyncUnaryCall(method, host: null, WithSubject(subject), request);
        return await call.ResponseAsync.ConfigureAwait(false);
    }

    [Test]
    public async Task scan_entries_returns_only_prefix_permitted_keys_for_the_reader()
    {
        var response = await CallAsync(
            _host.Methods.ScanEntries,
            new EntryScanRequest { TreeId = TreeA, PageSize = 100 },
            Reader);

        Assert.Multiple(() =>
        {
            Assert.That(response.Status, Is.EqualTo(StateQueryStatus.Found));
            Assert.That(response.Entries.Select(e => e.Key), Is.EquivalentTo(new[] { "x/1", "x/2" }));
        });
    }

    [Test]
    public async Task get_entry_on_an_unauthorised_key_reads_back_not_found_for_the_reader()
    {
        // The reader can read treeA under "x/" but not key "y/1"; per-key
        // enforcement denies it, which the facade surfaces as key-not-found. The
        // structured typed not-found (issue #1339 Finding 1) preserves existence-
        // hiding: the reader sees the same KeyNotFound status a genuine miss
        // returns, with no entry, and no leak of whether "y/1" exists.
        var response = await CallAsync(
            _host.Methods.GetEntry,
            new EntryGetRequest { TreeId = TreeA, Key = "y/1" },
            Reader);

        Assert.Multiple(() =>
        {
            Assert.That(response.Status, Is.EqualTo(StateQueryStatus.KeyNotFound));
            Assert.That(response.Entry, Is.Null);
        });
    }

    [Test]
    public async Task get_entry_on_a_permitted_key_returns_the_record_for_the_reader()
    {
        var response = await CallAsync(
            _host.Methods.GetEntry,
            new EntryGetRequest { TreeId = TreeA, Key = "x/1" },
            Reader);

        Assert.Multiple(() =>
        {
            Assert.That(response.Status, Is.EqualTo(StateQueryStatus.Found));
            Assert.That(response.Entry, Is.Not.Null);
        });
    }

    [Test]
    public async Task list_trees_omits_trees_the_reader_cannot_read()
    {
        var page = await CallAsync(
            _host.Methods.ListTrees,
            new CatalogRequest { PageSize = 100 },
            Reader);

        var treeIds = page.Entries.Select(e => e.TreeId).ToList();
        Assert.Multiple(() =>
        {
            Assert.That(treeIds, Does.Contain(TreeA));
            Assert.That(treeIds, Does.Not.Contain(TreeB));
        });
    }

    [Test]
    public void scan_entries_without_a_credential_header_fails_closed()
    {
        // An unresolved caller cannot see the tree at all, so the fully-hidden
        // tree reads back as tree-not-found (RpcException NotFound over the wire)
        // rather than leaking an empty-but-existing page.
        var ex = Assert.ThrowsAsync<RpcException>(async () => await CallAsync(
            _host.Methods.ScanEntries,
            new EntryScanRequest { TreeId = TreeA, PageSize = 100 },
            subject: null));

        Assert.That(ex!.StatusCode, Is.EqualTo(StatusCode.NotFound));
    }

    [Test]
    public async Task get_entry_without_a_credential_header_fails_closed()
    {
        // An unresolved caller cannot see the tree at all, so the fully-hidden tree
        // reads back as tree-not-found rather than leaking the record. The typed
        // not-found (issue #1339 Finding 1) preserves fail-closed hiding: the
        // response is TreeNotFound with no entry, identical to an unknown tree.
        var response = await CallAsync(
            _host.Methods.GetEntry,
            new EntryGetRequest { TreeId = TreeA, Key = "x/1" },
            subject: null);

        Assert.Multiple(() =>
        {
            Assert.That(response.Status, Is.EqualTo(StateQueryStatus.TreeNotFound));
            Assert.That(response.Entry, Is.Null);
        });
    }

    [Test]
    public async Task list_trees_without_a_credential_header_omits_all_trees()
    {
        var page = await CallAsync(
            _host.Methods.ListTrees,
            new CatalogRequest { PageSize = 100 },
            subject: null);

        var treeIds = page.Entries.Select(e => e.TreeId).ToList();
        Assert.Multiple(() =>
        {
            Assert.That(treeIds, Does.Not.Contain(TreeA));
            Assert.That(treeIds, Does.Not.Contain(TreeB));
        });
    }

    [Test]
    public async Task bootstrap_admin_sees_every_tree_over_the_wire()
    {
        var page = await CallAsync(
            _host.Methods.ListTrees,
            new CatalogRequest { PageSize = 100 },
            AuthGrpcStateClusterFixture.BootstrapAdmin);

        var treeIds = page.Entries.Select(e => e.TreeId).ToList();
        Assert.Multiple(() =>
        {
            Assert.That(treeIds, Does.Contain(TreeA));
            Assert.That(treeIds, Does.Contain(TreeB));
        });
    }
}

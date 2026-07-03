using Grpc.Core;
using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Api.Data.Grpc.Tests;

/// <summary>
/// End-to-end coverage for the write-capable data-API gRPC surface driven over an
/// in-process <c>TestServer</c> whose silo runs the enforcing
/// <see cref="ILatticeAccessGate"/>. Asserts that the caller's identity - carried
/// purely in the request's credential header and bridged onto the ambient
/// <see cref="LatticeCredentialContext"/> - scopes every mutation and read on the
/// wire: an authorized write is durable, an unauthorized or anonymous write is
/// denied with <see cref="StatusCode.PermissionDenied"/> (carrying only the
/// non-sensitive tree / operation / subject / reason trailers), an atomic and a
/// cross-tree batch abort wholesale on a denied leg with no partial state, a
/// point read of a denied key reads back absent, and a bounded range prunes to
/// the authorized subset.
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class AuthGrpcDataTests
{
    private const string Writer = "grpc-writer";
    private const string Reader = "grpc-reader";

    private AuthGrpcDataClusterFixture _fixture = null!;
    private GrpcDataHost _host = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new AuthGrpcDataClusterFixture();
        await _fixture.InitializeAsync();
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
            { "authorization", $"{AuthGrpcDataClusterFixture.CredentialScheme} {subject}" },
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

    private LatticeAuthorizationRule AllowTree(string subject, string treeId, LatticeOperation ops) =>
        new($"{subject}-{treeId}-tree", LatticeSubjectSelector.User(subject), LatticeScope.Tree(treeId), ops, LatticeEffect.Allow);

    private LatticeAuthorizationRule AllowPrefix(string subject, string treeId, string prefix, LatticeOperation ops) =>
        new($"{subject}-{treeId}-{prefix}", LatticeSubjectSelector.User(subject), LatticeScope.Prefix(treeId, prefix), ops, LatticeEffect.Allow);

    private const LatticeOperation WriteOps =
        LatticeOperation.Write | LatticeOperation.Delete | LatticeOperation.AtomicWrite;

    private const LatticeOperation ReadOps =
        LatticeOperation.Read | LatticeOperation.RangeRead;

    [Test]
    public async Task authorized_set_over_the_wire_is_durable()
    {
        const string tree = "grpc-set-ok";
        await _fixture.RegisterTreeAsync(tree);
        await _fixture.GrantAsync(AllowTree(Writer, tree, WriteOps));

        await CallAsync(
            _host.Methods.Set,
            new DataSetRequest { TreeId = tree, Key = "k1", Value = new byte[] { 42 } },
            Writer);

        var durable = await _fixture.ReadRawAsync(tree, "k1");
        Assert.That(durable, Is.EqualTo(new byte[] { 42 }));
    }

    [Test]
    public async Task unauthorized_set_over_the_wire_is_permission_denied_with_trailers()
    {
        const string tree = "grpc-set-denied";
        await _fixture.RegisterTreeAsync(tree);

        var ex = Assert.ThrowsAsync<RpcException>(async () => await CallAsync(
            _host.Methods.Set,
            new DataSetRequest { TreeId = tree, Key = "k1", Value = new byte[] { 1 } },
            Writer));

        Assert.Multiple(() =>
        {
            Assert.That(ex!.StatusCode, Is.EqualTo(StatusCode.PermissionDenied));

            // The denial carries only the non-sensitive tree / operation / subject
            // / reason context as trailers - never a value.
            var trailers = ex.Trailers;
            Assert.That(trailers.GetValue(LatticeDataApiGrpcService.DeniedTreeTrailer), Is.EqualTo(tree));
            Assert.That(trailers.GetValue(LatticeDataApiGrpcService.DeniedSubjectTrailer), Is.EqualTo(Writer));
            Assert.That(trailers.GetValue(LatticeDataApiGrpcService.DeniedOperationTrailer), Is.Not.Null);
            Assert.That(trailers.GetValue(LatticeDataApiGrpcService.DeniedReasonTrailer), Is.Not.Null);
        });

        Assert.That(await _fixture.ReadRawAsync(tree, "k1"), Is.Null);
    }

    [Test]
    public async Task anonymous_set_over_the_wire_is_permission_denied()
    {
        const string tree = "grpc-set-anon";
        await _fixture.RegisterTreeAsync(tree);

        var ex = Assert.ThrowsAsync<RpcException>(async () => await CallAsync(
            _host.Methods.Set,
            new DataSetRequest { TreeId = tree, Key = "k1", Value = new byte[] { 1 } },
            subject: null));

        Assert.That(ex!.StatusCode, Is.EqualTo(StatusCode.PermissionDenied));
        Assert.That(await _fixture.ReadRawAsync(tree, "k1"), Is.Null);
    }

    [Test]
    public async Task authorized_delete_over_the_wire_reports_removed()
    {
        const string tree = "grpc-delete-ok";
        await _fixture.RegisterTreeAsync(tree);
        await _fixture.GrantAsync(AllowTree(Writer, tree, WriteOps));

        await CallAsync(_host.Methods.Set, new DataSetRequest { TreeId = tree, Key = "k1", Value = new byte[] { 1 } }, Writer);
        var response = await CallAsync(_host.Methods.Delete, new DataDeleteRequest { TreeId = tree, Key = "k1" }, Writer);

        Assert.Multiple(() =>
        {
            Assert.That(response.Removed, Is.True);
            Assert.That(_fixture.ReadRawAsync(tree, "k1").Result, Is.Null);
        });
    }

    [Test]
    public async Task authorized_get_over_the_wire_returns_the_value()
    {
        const string tree = "grpc-get-ok";
        await _fixture.RegisterTreeAsync(tree);
        await _fixture.GrantAsync(AllowTree(Writer, tree, WriteOps | ReadOps));

        await CallAsync(_host.Methods.Set, new DataSetRequest { TreeId = tree, Key = "k1", Value = new byte[] { 7 } }, Writer);
        var result = await CallAsync(_host.Methods.Get, new DataGetRequest { TreeId = tree, Key = "k1" }, Writer);

        Assert.Multiple(() =>
        {
            Assert.That(result.Found, Is.True);
            Assert.That(result.Value, Is.EqualTo(new byte[] { 7 }));
        });
    }

    [Test]
    public async Task point_read_of_a_denied_key_over_the_wire_reads_back_absent()
    {
        const string tree = "grpc-read-scope";
        await _fixture.RegisterTreeAsync(tree);
        await _fixture.GrantAsync(AllowTree(Writer, tree, WriteOps));
        await CallAsync(_host.Methods.Set, new DataSetRequest { TreeId = tree, Key = "x/1", Value = new byte[] { 1 } }, Writer);
        await CallAsync(_host.Methods.Set, new DataSetRequest { TreeId = tree, Key = "y/1", Value = new byte[] { 2 } }, Writer);

        await _fixture.GrantAsync(AllowPrefix(Reader, tree, "x/", ReadOps));

        var permitted = await CallAsync(_host.Methods.Get, new DataGetRequest { TreeId = tree, Key = "x/1" }, Reader);
        var denied = await CallAsync(_host.Methods.Get, new DataGetRequest { TreeId = tree, Key = "y/1" }, Reader);

        Assert.Multiple(() =>
        {
            Assert.That(permitted.Found, Is.True);
            Assert.That(denied.Found, Is.False, "a key outside the reader's grant reads back absent, never a value");
        });
    }

    [Test]
    public async Task anonymous_get_over_the_wire_reads_back_absent()
    {
        const string tree = "grpc-get-anon";
        await _fixture.RegisterTreeAsync(tree);
        await _fixture.GrantAsync(AllowTree(Writer, tree, WriteOps));
        await CallAsync(_host.Methods.Set, new DataSetRequest { TreeId = tree, Key = "k1", Value = new byte[] { 9 } }, Writer);

        var result = await CallAsync(_host.Methods.Get, new DataGetRequest { TreeId = tree, Key = "k1" }, subject: null);

        Assert.That(result.Found, Is.False);
    }

    [Test]
    public async Task atomic_batch_over_the_wire_aborts_wholesale_on_a_denied_leg()
    {
        const string tree = "grpc-atomic-denied";
        await _fixture.RegisterTreeAsync(tree);
        await _fixture.GrantAsync(AllowPrefix(Writer, tree, "a/", WriteOps));

        var request = new DataAtomicRequest
        {
            TreeId = tree,
            OperationId = "grpc-atomic-op-1",
            Batch = new DataAtomicBatch
            {
                Upserts =
                [
                    new DataEntry { Key = "a/1", Value = new byte[] { 1 } },
                    new DataEntry { Key = "b/1", Value = new byte[] { 2 } },
                ],
            },
        };

        var ex = Assert.ThrowsAsync<RpcException>(async () => await CallAsync(
            _host.Methods.SetManyAtomic, request, Writer));

        Assert.Multiple(() =>
        {
            Assert.That(ex!.StatusCode, Is.EqualTo(StatusCode.PermissionDenied));
            Assert.That(_fixture.ReadRawAsync(tree, "a/1").Result, Is.Null, "no partial state after a denied atomic batch");
            Assert.That(_fixture.ReadRawAsync(tree, "b/1").Result, Is.Null);
        });
    }

    [Test]
    public async Task authorized_atomic_batch_over_the_wire_commits()
    {
        const string tree = "grpc-atomic-ok";
        await _fixture.RegisterTreeAsync(tree);
        await _fixture.GrantAsync(AllowTree(Writer, tree, WriteOps));

        var request = new DataAtomicRequest
        {
            TreeId = tree,
            OperationId = "grpc-atomic-ok-1",
            Batch = new DataAtomicBatch
            {
                Upserts =
                [
                    new DataEntry { Key = "a/1", Value = new byte[] { 1 } },
                    new DataEntry { Key = "a/2", Value = new byte[] { 2 } },
                ],
            },
        };

        await CallAsync(_host.Methods.SetManyAtomic, request, Writer);

        Assert.Multiple(() =>
        {
            Assert.That(_fixture.ReadRawAsync(tree, "a/1").Result, Is.EqualTo(new byte[] { 1 }));
            Assert.That(_fixture.ReadRawAsync(tree, "a/2").Result, Is.EqualTo(new byte[] { 2 }));
        });
    }

    [Test]
    public async Task cross_tree_atomic_over_the_wire_aborts_on_a_denied_leg()
    {
        const string treeA = "grpc-xt-a";
        const string treeB = "grpc-xt-b";
        await _fixture.RegisterTreeAsync(treeA);
        await _fixture.RegisterTreeAsync(treeB);
        await _fixture.GrantAsync(AllowTree(Writer, treeA, WriteOps));

        var request = new DataCrossTreeRequest
        {
            OperationId = "grpc-xt-op-1",
            Batches =
            [
                new DataTreeBatch { TreeId = treeA, Upserts = [new DataEntry { Key = "k", Value = new byte[] { 1 } }] },
                new DataTreeBatch { TreeId = treeB, Upserts = [new DataEntry { Key = "k", Value = new byte[] { 2 } }] },
            ],
        };

        var ex = Assert.ThrowsAsync<RpcException>(async () => await CallAsync(
            _host.Methods.SetManyAtomicCrossTree, request, Writer));

        Assert.Multiple(() =>
        {
            Assert.That(ex!.StatusCode, Is.EqualTo(StatusCode.PermissionDenied));
            Assert.That(_fixture.ReadRawAsync(treeA, "k").Result, Is.Null);
            Assert.That(_fixture.ReadRawAsync(treeB, "k").Result, Is.Null);
        });
    }

    [Test]
    public async Task authorized_cross_tree_atomic_over_the_wire_commits_every_tree()
    {
        const string treeA = "grpc-xt-ok-a";
        const string treeB = "grpc-xt-ok-b";
        await _fixture.RegisterTreeAsync(treeA);
        await _fixture.RegisterTreeAsync(treeB);
        await _fixture.GrantAsync(
            AllowTree(Writer, treeA, WriteOps),
            AllowTree(Writer, treeB, WriteOps));

        var request = new DataCrossTreeRequest
        {
            OperationId = "grpc-xt-ok-1",
            Batches =
            [
                new DataTreeBatch { TreeId = treeA, Upserts = [new DataEntry { Key = "k", Value = new byte[] { 1 } }] },
                new DataTreeBatch { TreeId = treeB, Upserts = [new DataEntry { Key = "k", Value = new byte[] { 2 } }] },
            ],
        };

        var response = await CallAsync(_host.Methods.SetManyAtomicCrossTree, request, Writer);

        Assert.Multiple(() =>
        {
            Assert.That(response.Outcome, Is.EqualTo(CrossTreeAtomicWriteOutcome.Committed));
            Assert.That(_fixture.ReadRawAsync(treeA, "k").Result, Is.EqualTo(new byte[] { 1 }));
            Assert.That(_fixture.ReadRawAsync(treeB, "k").Result, Is.EqualTo(new byte[] { 2 }));
        });
    }

    [Test]
    public async Task bounded_range_read_over_the_wire_prunes_to_the_authorized_subset()
    {
        const string tree = "grpc-range-scope";
        await _fixture.RegisterTreeAsync(tree);
        await _fixture.GrantAsync(AllowTree(Writer, tree, WriteOps));
        await CallAsync(_host.Methods.Set, new DataSetRequest { TreeId = tree, Key = "x/1", Value = new byte[] { 1 } }, Writer);
        await CallAsync(_host.Methods.Set, new DataSetRequest { TreeId = tree, Key = "x/2", Value = new byte[] { 2 } }, Writer);
        await CallAsync(_host.Methods.Set, new DataSetRequest { TreeId = tree, Key = "y/1", Value = new byte[] { 3 } }, Writer);

        await _fixture.GrantAsync(AllowPrefix(Reader, tree, "x/", ReadOps));

        var page = await CallAsync(
            _host.Methods.ReadRange,
            new DataRangeRequest { TreeId = tree, PageSize = 100 },
            Reader);

        Assert.That(page.Entries.Select(e => e.Key), Is.EquivalentTo(new[] { "x/1", "x/2" }));
    }

    [Test]
    public async Task identity_bridge_maps_the_header_credential_to_the_writing_subject()
    {
        const string tree = "grpc-identity";
        await _fixture.RegisterTreeAsync(tree);

        // Only "grpc-writer" is granted; the same call made as a different subject
        // header must be denied, proving the header credential (not some ambient
        // default) is what the gate reasons over.
        await _fixture.GrantAsync(AllowTree(Writer, tree, WriteOps));

        await CallAsync(_host.Methods.Set, new DataSetRequest { TreeId = tree, Key = "k1", Value = new byte[] { 1 } }, Writer);

        var ex = Assert.ThrowsAsync<RpcException>(async () => await CallAsync(
            _host.Methods.Set,
            new DataSetRequest { TreeId = tree, Key = "k2", Value = new byte[] { 2 } },
            "some-other-subject"));

        Assert.Multiple(() =>
        {
            Assert.That(_fixture.ReadRawAsync(tree, "k1").Result, Is.EqualTo(new byte[] { 1 }));
            Assert.That(ex!.StatusCode, Is.EqualTo(StatusCode.PermissionDenied));
            Assert.That(_fixture.ReadRawAsync(tree, "k2").Result, Is.Null);
        });
    }
}

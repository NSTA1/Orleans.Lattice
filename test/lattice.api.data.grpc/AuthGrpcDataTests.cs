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

    private const LatticeOperation CrdtOps =
        LatticeOperation.CrdtApply | LatticeOperation.Read;

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

        await Assert.MultipleAsync(async () =>
        {
            Assert.That(response.Removed, Is.True);
            Assert.That(await _fixture.ReadRawAsync(tree, "k1"), Is.Null);
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

        await Assert.MultipleAsync(async () =>
        {
            Assert.That(ex!.StatusCode, Is.EqualTo(StatusCode.PermissionDenied));
            Assert.That(await _fixture.ReadRawAsync(tree, "a/1"), Is.Null, "no partial state after a denied atomic batch");
            Assert.That(await _fixture.ReadRawAsync(tree, "b/1"), Is.Null);
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

        await Assert.MultipleAsync(async () =>
        {
            Assert.That(await _fixture.ReadRawAsync(tree, "a/1"), Is.EqualTo(new byte[] { 1 }));
            Assert.That(await _fixture.ReadRawAsync(tree, "a/2"), Is.EqualTo(new byte[] { 2 }));
        });
    }

    [Test]
    public async Task atomic_batch_reusing_operationId_with_a_different_key_set_maps_to_failed_precondition()
    {
        const string tree = "grpc-atomic-mismatch";
        await _fixture.RegisterTreeAsync(tree);
        await _fixture.GrantAsync(AllowTree(Writer, tree, WriteOps));

        var first = new DataAtomicRequest
        {
            TreeId = tree,
            OperationId = "grpc-atomic-mismatch-1",
            Batch = new DataAtomicBatch
            {
                Upserts = [new DataEntry { Key = "a/1", Value = new byte[] { 1 } }],
            },
        };
        await CallAsync(_host.Methods.SetManyAtomic, first, Writer);

        // Re-submitting the same operationId with a different key set is a caller
        // error, not a server fault (issue #1396). It maps to FailedPrecondition
        // with a self-contained message that never mentions cluster logs, and
        // nothing from the second batch is applied.
        var mismatched = new DataAtomicRequest
        {
            TreeId = tree,
            OperationId = "grpc-atomic-mismatch-1",
            Batch = new DataAtomicBatch
            {
                Upserts = [new DataEntry { Key = "b/1", Value = new byte[] { 9 } }],
            },
        };

        var ex = Assert.ThrowsAsync<RpcException>(async () => await CallAsync(
            _host.Methods.SetManyAtomic, mismatched, Writer));

        await Assert.MultipleAsync(async () =>
        {
            Assert.That(ex!.StatusCode, Is.EqualTo(StatusCode.FailedPrecondition));
            Assert.That(ex.Status.Detail, Does.Contain("different set of keys"));
            Assert.That(ex.Status.Detail, Does.Not.Contain("cluster logs"));
            Assert.That(await _fixture.ReadRawAsync(tree, "b/1"), Is.Null,
                "no partial state after a rejected key-set-mismatch retry");
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

        await Assert.MultipleAsync(async () =>
        {
            Assert.That(ex!.StatusCode, Is.EqualTo(StatusCode.PermissionDenied));
            Assert.That(await _fixture.ReadRawAsync(treeA, "k"), Is.Null);
            Assert.That(await _fixture.ReadRawAsync(treeB, "k"), Is.Null);
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

        await Assert.MultipleAsync(async () =>
        {
            Assert.That(response.Outcome, Is.EqualTo(CrossTreeAtomicWriteOutcome.Committed));
            Assert.That(await _fixture.ReadRawAsync(treeA, "k"), Is.EqualTo(new byte[] { 1 }));
            Assert.That(await _fixture.ReadRawAsync(treeB, "k"), Is.EqualTo(new byte[] { 2 }));
        });
    }

    [Test]
    public async Task cross_tree_atomic_reusing_operationId_with_a_changed_tree_or_key_set_is_failed_precondition()
    {
        // Issue #1402 item 2: the cross-tree atomic write must reject a reused
        // operationId whose tree/key set changed as a caller precondition (just as
        // the single-tree variant does), surfacing FailedPrecondition over the wire
        // with nothing from the mismatched retry applied.
        const string treeA = "grpc-xt-idem-a";
        const string treeB = "grpc-xt-idem-b";
        await _fixture.RegisterTreeAsync(treeA);
        await _fixture.RegisterTreeAsync(treeB);
        await _fixture.GrantAsync(
            AllowTree(Writer, treeA, WriteOps),
            AllowTree(Writer, treeB, WriteOps));

        var first = new DataCrossTreeRequest
        {
            OperationId = "grpc-xt-idem-1",
            Batches =
            [
                new DataTreeBatch { TreeId = treeA, Upserts = [new DataEntry { Key = "k", Value = new byte[] { 1 } }] },
                new DataTreeBatch { TreeId = treeB, Upserts = [new DataEntry { Key = "k", Value = new byte[] { 2 } }] },
            ],
        };
        await CallAsync(_host.Methods.SetManyAtomicCrossTree, first, Writer);

        var mismatched = new DataCrossTreeRequest
        {
            OperationId = "grpc-xt-idem-1",
            Batches =
            [
                new DataTreeBatch { TreeId = treeA, Upserts = [new DataEntry { Key = "different", Value = new byte[] { 9 } }] },
                new DataTreeBatch { TreeId = treeB, Upserts = [new DataEntry { Key = "k", Value = new byte[] { 2 } }] },
            ],
        };

        var ex = Assert.ThrowsAsync<RpcException>(async () => await CallAsync(
            _host.Methods.SetManyAtomicCrossTree, mismatched, Writer));

        await Assert.MultipleAsync(async () =>
        {
            Assert.That(ex!.StatusCode, Is.EqualTo(StatusCode.FailedPrecondition));
            Assert.That(ex.Status.Detail, Does.Not.Contain("cluster logs"));
            Assert.That(await _fixture.ReadRawAsync(treeA, "different"), Is.Null,
                "no partial state after a rejected key-set-mismatch retry");
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
    public async Task authorized_range_delete_over_the_wire_drains_the_whole_range()
    {
        const string tree = "grpc-range-delete-ok";
        await _fixture.RegisterTreeAsync(tree);
        await _fixture.GrantAsync(AllowTree(Writer, tree, WriteOps | LatticeOperation.RangeDelete | ReadOps));

        for (var i = 0; i < 5; i++)
        {
            await CallAsync(_host.Methods.Set, new DataSetRequest { TreeId = tree, Key = $"k{i:D2}", Value = new byte[] { (byte)i } }, Writer);
        }
        await CallAsync(_host.Methods.Set, new DataSetRequest { TreeId = tree, Key = "zzz", Value = new byte[] { 99 } }, Writer);

        var result = await CallAsync(
            _host.Methods.DeleteRange,
            new DataRangeDeleteRequest { TreeId = tree, StartInclusive = "k00", EndExclusive = "k99" },
            Writer);

        await Assert.MultipleAsync(async () =>
        {
            Assert.That(result.DeletedCount, Is.EqualTo(5));
            Assert.That(await _fixture.ReadRawAsync(tree, "k00"), Is.Null);
            Assert.That(await _fixture.ReadRawAsync(tree, "zzz"), Is.EqualTo(new byte[] { 99 }));
        });
    }

    [Test]
    public async Task unauthorized_range_delete_over_the_wire_is_permission_denied()
    {
        const string tree = "grpc-range-delete-denied";
        await _fixture.RegisterTreeAsync(tree);

        // Writer may write and point-delete, but has no RangeDelete grant.
        await _fixture.GrantAsync(AllowTree(Writer, tree, WriteOps));
        await CallAsync(_host.Methods.Set, new DataSetRequest { TreeId = tree, Key = "k1", Value = new byte[] { 1 } }, Writer);

        var ex = Assert.ThrowsAsync<RpcException>(async () => await CallAsync(
            _host.Methods.DeleteRange,
            new DataRangeDeleteRequest { TreeId = tree, StartInclusive = "k0", EndExclusive = "k9" },
            Writer));

        await Assert.MultipleAsync(async () =>
        {
            Assert.That(ex!.StatusCode, Is.EqualTo(StatusCode.PermissionDenied));
            Assert.That(await _fixture.ReadRawAsync(tree, "k1"), Is.EqualTo(new byte[] { 1 }));
        });
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

        await Assert.MultipleAsync(async () =>
        {
            Assert.That(await _fixture.ReadRawAsync(tree, "k1"), Is.EqualTo(new byte[] { 1 }));
            Assert.That(ex!.StatusCode, Is.EqualTo(StatusCode.PermissionDenied));
            Assert.That(await _fixture.ReadRawAsync(tree, "k2"), Is.Null);
        });
    }

    [Test]
    public async Task set_many_over_the_wire_writes_every_key()
    {
        const string tree = "grpc-set-many";
        await _fixture.RegisterTreeAsync(tree);
        await _fixture.GrantAsync(AllowTree(Writer, tree, WriteOps));

        await CallAsync(
            _host.Methods.SetMany,
            new DataSetManyRequest
            {
                TreeId = tree,
                Upserts =
                {
                    new DataEntry { Key = "a", Value = new byte[] { 1 } },
                    new DataEntry { Key = "b", Value = new byte[] { 2 } },
                },
            },
            Writer);

        await Assert.MultipleAsync(async () =>
        {
            Assert.That(await _fixture.ReadRawAsync(tree, "a"), Is.EqualTo(new byte[] { 1 }));
            Assert.That(await _fixture.ReadRawAsync(tree, "b"), Is.EqualTo(new byte[] { 2 }));
        });
    }

    [Test]
    public async Task set_many_over_the_wire_on_a_denied_key_is_permission_denied()
    {
        const string tree = "grpc-set-many-denied";
        await _fixture.RegisterTreeAsync(tree);

        var ex = Assert.ThrowsAsync<RpcException>(async () => await CallAsync(
            _host.Methods.SetMany,
            new DataSetManyRequest { TreeId = tree, Upserts = { new DataEntry { Key = "a", Value = new byte[] { 1 } } } },
            Writer));

        await Assert.MultipleAsync(async () =>
        {
            Assert.That(ex!.StatusCode, Is.EqualTo(StatusCode.PermissionDenied));
            Assert.That(await _fixture.ReadRawAsync(tree, "a"), Is.Null);
        });
    }

    [Test]
    public async Task crdt_counter_write_then_read_over_the_wire_round_trips()
    {
        const string tree = "grpc-crdt-counter";
        await _fixture.RegisterTreeAsync(tree);
        await _fixture.GrantAsync(AllowTree(Writer, tree, CrdtOps));

        await CallAsync(
            _host.Methods.CrdtWrite,
            new CrdtWriteRequest { TreeId = tree, Key = "c", Op = CrdtWriteOp.CounterIncrement, ReplicaId = "r1", Amount = 4 },
            Writer);
        await CallAsync(
            _host.Methods.CrdtWrite,
            new CrdtWriteRequest { TreeId = tree, Key = "c", Op = CrdtWriteOp.CounterDecrement, ReplicaId = "r2", Amount = 1 },
            Writer);

        var read = await CallAsync(
            _host.Methods.CrdtRead,
            new CrdtReadRequest { TreeId = tree, Key = "c", Kind = CrdtKind.PnCounter },
            Writer);

        Assert.That(read.CounterValue, Is.EqualTo(3));
    }

    [Test]
    public async Task crdt_orset_write_then_read_over_the_wire_round_trips()
    {
        const string tree = "grpc-crdt-orset";
        await _fixture.RegisterTreeAsync(tree);
        await _fixture.GrantAsync(AllowTree(Writer, tree, CrdtOps));

        await CallAsync(
            _host.Methods.CrdtWrite,
            new CrdtWriteRequest { TreeId = tree, Key = "s", Op = CrdtWriteOp.SetAdd, ReplicaId = "r1", Element = new byte[] { 7 } },
            Writer);

        var read = await CallAsync(
            _host.Methods.CrdtRead,
            new CrdtReadRequest { TreeId = tree, Key = "s", Kind = CrdtKind.OrSet },
            Writer);

        Assert.That(read.Elements, Has.Count.EqualTo(1).And.ItemAt(0).EqualTo(new byte[] { 7 }));
    }

    [Test]
    public async Task crdt_max_register_write_then_read_over_the_wire_round_trips()
    {
        const string tree = "grpc-crdt-maxregister";
        await _fixture.RegisterTreeAsync(tree);
        await _fixture.GrantAsync(AllowTree(Writer, tree, CrdtOps));

        await CallAsync(
            _host.Methods.CrdtWrite,
            new CrdtWriteRequest { TreeId = tree, Key = "r", Op = CrdtWriteOp.MaxRegisterSet, Element = new byte[] { 0x02 } },
            Writer);
        await CallAsync(
            _host.Methods.CrdtWrite,
            new CrdtWriteRequest { TreeId = tree, Key = "r", Op = CrdtWriteOp.MaxRegisterSet, Element = new byte[] { 0x08 } },
            Writer);

        var read = await CallAsync(
            _host.Methods.CrdtRead,
            new CrdtReadRequest { TreeId = tree, Key = "r", Kind = CrdtKind.MaxRegister },
            Writer);

        Assert.That(read.Elements, Has.Count.EqualTo(1).And.ItemAt(0).EqualTo(new byte[] { 0x08 }));
    }

    [Test]
    public async Task crdt_min_register_write_then_read_over_the_wire_round_trips()
    {
        const string tree = "grpc-crdt-minregister";
        await _fixture.RegisterTreeAsync(tree);
        await _fixture.GrantAsync(AllowTree(Writer, tree, CrdtOps));

        await CallAsync(
            _host.Methods.CrdtWrite,
            new CrdtWriteRequest { TreeId = tree, Key = "r", Op = CrdtWriteOp.MinRegisterSet, Element = new byte[] { 0x08 } },
            Writer);
        await CallAsync(
            _host.Methods.CrdtWrite,
            new CrdtWriteRequest { TreeId = tree, Key = "r", Op = CrdtWriteOp.MinRegisterSet, Element = new byte[] { 0x02 } },
            Writer);

        var read = await CallAsync(
            _host.Methods.CrdtRead,
            new CrdtReadRequest { TreeId = tree, Key = "r", Kind = CrdtKind.MinRegister },
            Writer);

        Assert.That(read.Elements, Has.Count.EqualTo(1).And.ItemAt(0).EqualTo(new byte[] { 0x02 }));
    }

    [Test]
    public async Task crdt_write_over_the_wire_on_a_denied_key_is_permission_denied()
    {
        const string tree = "grpc-crdt-denied";
        await _fixture.RegisterTreeAsync(tree);

        var ex = Assert.ThrowsAsync<RpcException>(async () => await CallAsync(
            _host.Methods.CrdtWrite,
            new CrdtWriteRequest { TreeId = tree, Key = "c", Op = CrdtWriteOp.CounterIncrement, ReplicaId = "r1", Amount = 1 },
            Writer));

        Assert.Multiple(() =>
        {
            Assert.That(ex!.StatusCode, Is.EqualTo(StatusCode.PermissionDenied));
            Assert.That(ex!.Trailers.GetValue(LatticeDataApiGrpcService.DeniedTreeTrailer), Is.EqualTo(tree));
        });
    }

    [Test]
    public async Task crdt_read_over_the_wire_of_a_denied_key_reads_back_empty()
    {
        const string tree = "grpc-crdt-read-denied";
        await _fixture.RegisterTreeAsync(tree);
        await _fixture.GrantAsync(AllowTree(Writer, tree, CrdtOps));
        await CallAsync(
            _host.Methods.CrdtWrite,
            new CrdtWriteRequest { TreeId = tree, Key = "c", Op = CrdtWriteOp.CounterIncrement, ReplicaId = "r1", Amount = 5 },
            Writer);

        // Reader has no grant: a fail-closed read reads the empty value, not a fault.
        var read = await CallAsync(
            _host.Methods.CrdtRead,
            new CrdtReadRequest { TreeId = tree, Key = "c", Kind = CrdtKind.PnCounter },
            Reader);

        Assert.That(read.CounterValue, Is.EqualTo(0));
    }

    [Test]
    public async Task crdt_map_write_on_a_tree_with_no_registered_shape_is_failed_precondition()
    {
        // The default gRPC-host cluster registers no OR-Map (TKey, TValue) shape,
        // so an OR-Map verb is a deterministic host-configuration precondition -
        // it must surface as FailedPrecondition (not an opaque Internal fault).
        const string tree = "grpc-crdt-map-noshape";
        await _fixture.RegisterTreeAsync(tree);
        await _fixture.GrantAsync(AllowTree(Writer, tree, CrdtOps));

        var ex = Assert.ThrowsAsync<RpcException>(async () => await CallAsync(
            _host.Methods.CrdtWrite,
            new CrdtWriteRequest
            {
                TreeId = tree,
                Key = "m",
                Op = CrdtWriteOp.MapSet,
                ReplicaId = "r1",
                Field = "f",
                Element = new byte[] { 9 },
            },
            Writer));

        Assert.Multiple(() =>
        {
            Assert.That(ex!.StatusCode, Is.EqualTo(StatusCode.FailedPrecondition));
            Assert.That(ex!.Status.Detail, Does.Contain("AddOrMapShape"));
        });
    }

    [Test]
    public async Task mismatched_shape_write_on_a_replicated_tree_is_failed_precondition_not_internal()
    {
        // A plain LWW Set targets a tree the silo declares as a replicated
        // PN-counter, so the origin write guard rejects it with
        // LatticeReplicationModeMismatchException before it commits. Across the
        // wire that deterministic caller/configuration precondition must surface
        // as FailedPrecondition with a self-contained message - not an opaque
        // Internal fault (issue #1402 item 5) - and nothing may be written.
        var tree = AuthGrpcDataClusterFixture.ReplicatedCounterPrefix + "grpc-1402";
        await _fixture.RegisterTreeAsync(tree);
        await _fixture.GrantAsync(AllowTree(Writer, tree, WriteOps));

        var ex = Assert.ThrowsAsync<RpcException>(async () => await CallAsync(
            _host.Methods.Set,
            new DataSetRequest { TreeId = tree, Key = "k1", Value = new byte[] { 1 } },
            Writer));

        Assert.Multiple(() =>
        {
            Assert.That(ex!.StatusCode, Is.EqualTo(StatusCode.FailedPrecondition));
            Assert.That(ex!.Status.Detail, Does.Not.Contain("cluster logs"));
        });

        Assert.That(await _fixture.ReadRawAsync(tree, "k1"), Is.Null);
    }

    [TestCase("t/victim/orders")]
    [TestCase("sys-auth-policy")]
    public void reserved_namespace_write_is_invalid_argument_not_internal(string reservedTreeId)
    {
        // Naming a tree inside a reserved, internally-composed namespace is a
        // deterministic caller-side precondition - that id is not addressable
        // through the public surface for anyone - so it must surface as a typed
        // client error. It previously threw a bare InvalidOperationException that
        // fell through to the generic server-fault arm, reporting a client error
        // as Internal and sending the caller to the cluster logs for something
        // they could see and fix themselves.
        var ex = Assert.ThrowsAsync<RpcException>(async () => await CallAsync(
            _host.Methods.Set,
            new DataSetRequest { TreeId = reservedTreeId, Key = "k1", Value = new byte[] { 1 } },
            Writer));

        Assert.Multiple(() =>
        {
            Assert.That(ex!.StatusCode, Is.EqualTo(StatusCode.InvalidArgument));
            Assert.That(ex!.Status.Detail, Does.Contain("reserved"));
            Assert.That(ex!.Status.Detail, Does.Not.Contain("cluster logs"),
                "a caller-fixable error must not be reported as an opaque server fault");
        });
    }
}

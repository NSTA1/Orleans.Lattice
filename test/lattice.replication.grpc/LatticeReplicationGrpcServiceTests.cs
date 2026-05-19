using Orleans.Lattice.BPlusTree.Grains;
using Grpc.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication;
using Orleans.Lattice.Replication.Grpc;
using Orleans.Serialization;

namespace Orleans.Lattice.Replication.Grpc.Tests;

[TestFixture]
public class LatticeReplicationGrpcServiceTests
{
    private static LatticeReplicationGrpcService CreateService(IReplicationApplier applier, out LatticeReplicationGrpcMethod method)
    {
        return CreateService(applier, new InMemoryWalCursorRegistry(), NoOpReceiverFlowControlPolicy.Instance, out method);
    }

    private static LatticeReplicationGrpcService CreateService(
        IReplicationApplier applier,
        IWalCursorRegistry cursorRegistry,
        out LatticeReplicationGrpcMethod method)
    {
        return CreateService(applier, cursorRegistry, NoOpReceiverFlowControlPolicy.Instance, out method);
    }

    private static LatticeReplicationGrpcService CreateService(
        IReplicationApplier applier,
        IWalCursorRegistry cursorRegistry,
        IReceiverFlowControlPolicy policy,
        out LatticeReplicationGrpcMethod method)
    {
        var sp = new ServiceCollection().AddSerializer().BuildServiceProvider();
        var ackSerializer = sp.GetRequiredService<Serializer<ReplicationAck>>();
        var envSerializer = sp.GetRequiredService<Serializer<ReplicationBatchEnvelope>>();
        var encoder = new TestEncoder(envSerializer);
        method = new LatticeReplicationGrpcMethod(encoder, ackSerializer);
        return new LatticeReplicationGrpcService(method, applier, cursorRegistry, policy, NullLogger<LatticeReplicationGrpcService>.Instance);
    }

    private sealed class TestEncoder : IReplicationBatchEncoder
    {
        private readonly Serializer<ReplicationBatchEnvelope> _s;
        public TestEncoder(Serializer<ReplicationBatchEnvelope> s) => _s = s;
        public string ContentType => "test/binary";
        public int CurrentWireVersion => 1;
        public void Encode(ReplicationBatchEnvelope envelope, System.Buffers.IBufferWriter<byte> writer) => _s.Serialize(envelope, writer);
        public ReplicationBatchEnvelope Decode(ReadOnlyMemory<byte> payload) => _s.Deserialize(payload.Span);
    }

    private static ServerCallContext MakeCallContext(CancellationToken ct = default)
    {
        return Substitute.For<ServerCallContext>();
        // NSubstitute can't substitute non-virtual ServerCallContext members fully;
        // but the service only reads CancellationToken, which is a property we can
        // configure on a TestServerCallContext.
    }

    private sealed class TestServerCallContext : ServerCallContext
    {
        private readonly CancellationToken _ct;
        public TestServerCallContext() : this(CancellationToken.None) { }
        public TestServerCallContext(CancellationToken ct) { _ct = ct; }
        protected override string MethodCore => "Push";
        protected override string HostCore => string.Empty;
        protected override string PeerCore => string.Empty;
        protected override DateTime DeadlineCore => DateTime.MaxValue;
        protected override global::Grpc.Core.Metadata RequestHeadersCore => new();
        protected override CancellationToken CancellationTokenCore => _ct;
        protected override global::Grpc.Core.Metadata ResponseTrailersCore => new();
        protected override Status StatusCore { get; set; }
        protected override WriteOptions? WriteOptionsCore { get; set; }
        protected override AuthContext AuthContextCore => null!;
        protected override ContextPropagationToken CreatePropagationTokenCore(ContextPropagationOptions? options) => null!;
        protected override Task WriteResponseHeadersAsyncCore(global::Grpc.Core.Metadata responseHeaders) => Task.CompletedTask;
    }

    private static WalRecord MakeSet(string key, HybridLogicalClock hlc, string origin = "remote")
        => new()
        {
            TreeId = "tree",
            Op = MutationKind.Set,
            Key = key,
            Value = new byte[] { 1 },
            Timestamp = hlc,
            OriginClusterId = origin,
            Mode = LatticeMergeMode.LwwRegister,
        };

    [Test]
    public void Constructor_throws_when_method_null()
    {
        Assert.That(
            () => new LatticeReplicationGrpcService(null!, Substitute.For<IReplicationApplier>(), new InMemoryWalCursorRegistry(), NoOpReceiverFlowControlPolicy.Instance, NullLogger<LatticeReplicationGrpcService>.Instance),
            Throws.ArgumentNullException);
    }

    [Test]
    public void Constructor_throws_when_applier_null()
    {
        var sp = new ServiceCollection().AddSerializer().BuildServiceProvider();
        var encoder = new TestEncoder(sp.GetRequiredService<Serializer<ReplicationBatchEnvelope>>());
        var method = new LatticeReplicationGrpcMethod(encoder, sp.GetRequiredService<Serializer<ReplicationAck>>());

        Assert.That(
            () => new LatticeReplicationGrpcService(method, null!, new InMemoryWalCursorRegistry(), NoOpReceiverFlowControlPolicy.Instance, NullLogger<LatticeReplicationGrpcService>.Instance),
            Throws.ArgumentNullException);
    }

    [Test]
    public void Constructor_throws_when_flow_control_policy_null()
    {
        var sp = new ServiceCollection().AddSerializer().BuildServiceProvider();
        var encoder = new TestEncoder(sp.GetRequiredService<Serializer<ReplicationBatchEnvelope>>());
        var method = new LatticeReplicationGrpcMethod(encoder, sp.GetRequiredService<Serializer<ReplicationAck>>());

        Assert.That(
            () => new LatticeReplicationGrpcService(method, Substitute.For<IReplicationApplier>(), new InMemoryWalCursorRegistry(), null!, NullLogger<LatticeReplicationGrpcService>.Instance),
            Throws.ArgumentNullException);
    }

    [Test]
    public void Constructor_throws_when_logger_null()
    {
        var sp = new ServiceCollection().AddSerializer().BuildServiceProvider();
        var encoder = new TestEncoder(sp.GetRequiredService<Serializer<ReplicationBatchEnvelope>>());
        var method = new LatticeReplicationGrpcMethod(encoder, sp.GetRequiredService<Serializer<ReplicationAck>>());

        Assert.That(
            () => new LatticeReplicationGrpcService(method, Substitute.For<IReplicationApplier>(), new InMemoryWalCursorRegistry(), NoOpReceiverFlowControlPolicy.Instance, null!),
            Throws.ArgumentNullException);
    }

    [Test]
    public void Push_throws_when_request_box_null()
    {
        var svc = CreateService(Substitute.For<IReplicationApplier>(), out _);
        Assert.That(
            async () => await svc.Push(null!, new TestServerCallContext()),
            Throws.ArgumentNullException);
    }

    [Test]
    public void Push_throws_when_tree_name_empty()
    {
        var svc = CreateService(Substitute.For<IReplicationApplier>(), out _);
        var box = new ReplicationBatchEnvelopeBox
        {
            Value = new ReplicationBatchEnvelope
            {
                TreeName = string.Empty,
                OriginClusterId = "x",
                Entries = Array.Empty<WalRecord>(),
            },
        };

        Assert.That(
            async () => await svc.Push(box, new TestServerCallContext()),
            Throws.TypeOf<RpcException>());
    }

    [Test]
    public void Push_throws_when_origin_empty()
    {
        var svc = CreateService(Substitute.For<IReplicationApplier>(), out _);
        var box = new ReplicationBatchEnvelopeBox
        {
            Value = new ReplicationBatchEnvelope
            {
                TreeName = "tree",
                OriginClusterId = string.Empty,
                Entries = Array.Empty<WalRecord>(),
            },
        };

        Assert.That(
            async () => await svc.Push(box, new TestServerCallContext()),
            Throws.TypeOf<RpcException>());
    }

    [Test]
    public async Task Push_returns_zero_hwm_for_empty_batch()
    {
        var applier = Substitute.For<IReplicationApplier>();
        var svc = CreateService(applier, out _);
        var box = new ReplicationBatchEnvelopeBox
        {
            Value = new ReplicationBatchEnvelope
            {
                TreeName = "tree",
                OriginClusterId = "remote",
                Entries = Array.Empty<WalRecord>(),
            },
        };

        var ack = await svc.Push(box, new TestServerCallContext());

        Assert.Multiple(() =>
        {
            Assert.That(ack.Value.Accepted, Is.True);
            Assert.That(ack.Value.HighestAppliedHlc, Is.EqualTo(HybridLogicalClock.Zero));
        });
    }

    [Test]
    public async Task Push_returns_max_hwm_across_entries()
    {
        var applier = Substitute.For<IReplicationApplier>();
        var hlcLow = new HybridLogicalClock { WallClockTicks = 100, Counter = 0 };
        var hlcHigh = new HybridLogicalClock { WallClockTicks = 200, Counter = 0 };
        var hlcMid = new HybridLogicalClock { WallClockTicks = 150, Counter = 0 };

        // The receiver-side service collapses per-entry HWM round-trips by
        // calling ApplyBatchAsync once per inbound push instead of looping
        // over ApplyAsync. The substitute does not route through the
        // default-interface-method body, so we set ApplyBatchAsync up
        // explicitly with the aggregate result the optimised batch path
        // would have computed (max HWM across the three entries).
        applier.ApplyBatchAsync(Arg.Any<IReadOnlyList<WalRecord>>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(new ApplyResult { Applied = true, HighWaterMark = hlcHigh }));

        var svc = CreateService(applier, out _);
        var box = new ReplicationBatchEnvelopeBox
        {
            Value = new ReplicationBatchEnvelope
            {
                TreeName = "tree",
                OriginClusterId = "remote",
                Entries = new[]
                {
                    MakeSet("a", hlcLow),
                    MakeSet("b", hlcHigh),
                    MakeSet("c", hlcMid),
                },
            },
        };

        var ack = await svc.Push(box, new TestServerCallContext());

        Assert.Multiple(() =>
        {
            Assert.That(ack.Value.Accepted, Is.True);
            Assert.That(ack.Value.HighestAppliedHlc, Is.EqualTo(hlcHigh));
        });
        await applier.Received(1).ApplyBatchAsync(
            Arg.Is<IReadOnlyList<WalRecord>>(list => list.Count == 3),
            Arg.Any<CancellationToken>());
    }

    [Test]
    public void Push_throws_rpc_exception_on_apply_failure()
    {
        var applier = Substitute.For<IReplicationApplier>();
        applier.ApplyBatchAsync(Arg.Any<IReadOnlyList<WalRecord>>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromException<ApplyResult>(new InvalidOperationException("bang")));

        var svc = CreateService(applier, out _);
        var box = new ReplicationBatchEnvelopeBox
        {
            Value = new ReplicationBatchEnvelope
            {
                TreeName = "tree",
                OriginClusterId = "remote",
                Entries = new[] { MakeSet("a", new HybridLogicalClock { WallClockTicks = 1, Counter = 0 }) },
            },
        };

        Assert.That(
            async () => await svc.Push(box, new TestServerCallContext()),
            Throws.TypeOf<RpcException>().With.Property("StatusCode").EqualTo(StatusCode.Internal));
    }

    [Test]
    public void Push_propagates_cancellation()
    {
        var applier = Substitute.For<IReplicationApplier>();
        // Honour the inbound cancellation token from the batch path so the
        // service's `catch (OperationCanceledException) when (...)` filter
        // re-throws the OCE instead of wrapping it as an RpcException.
        applier.ApplyBatchAsync(Arg.Any<IReadOnlyList<WalRecord>>(), Arg.Any<CancellationToken>())
            .Returns(callInfo =>
            {
                callInfo.Arg<CancellationToken>().ThrowIfCancellationRequested();
                return Task.FromResult(new ApplyResult { Applied = false, HighWaterMark = HybridLogicalClock.Zero });
            });

        var svc = CreateService(applier, out _);
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        var box = new ReplicationBatchEnvelopeBox
        {
            Value = new ReplicationBatchEnvelope
            {
                TreeName = "tree",
                OriginClusterId = "remote",
                Entries = new[] { MakeSet("a", new HybridLogicalClock { WallClockTicks = 1, Counter = 0 }) },
            },
        };

        Assert.That(
            async () => await svc.Push(box, new TestServerCallContext(cts.Token)),
            Throws.InstanceOf<OperationCanceledException>());
    }

    [Test]
    public void BindService_throws_when_binder_null()
    {
        Assert.That(
            () => LatticeReplicationGrpcServiceBase.BindService(null!, serviceImpl: null),
            Throws.ArgumentNullException);
    }

    [Test]
    public void BindService_throws_when_method_holder_not_initialised()
    {
        // Reset the holder to simulate a cold start before
        // AddLatticeReplicationGrpc / MapLatticeReplicationGrpc ran.
        var saved = LatticeReplicationGrpcMethodHolder.Current;
        LatticeReplicationGrpcMethodHolder.Current = null;
        try
        {
            Assert.That(
                () => LatticeReplicationGrpcServiceBase.BindService(Substitute.For<ServiceBinderBase>(), serviceImpl: null),
                Throws.InvalidOperationException);
        }
        finally
        {
            LatticeReplicationGrpcMethodHolder.Current = saved;
        }
    }

    [Test]
    public void BindService_records_metadata_when_service_impl_null()
    {
        // Ensure the holder is populated.
        CreateService(Substitute.For<IReplicationApplier>(), out var method);
        LatticeReplicationGrpcMethodHolder.Current = method;

        var binder = Substitute.For<ServiceBinderBase>();
        Assert.That(
            () => LatticeReplicationGrpcServiceBase.BindService(binder, serviceImpl: null),
            Throws.Nothing);
    }

    [Test]
    public void BindService_binds_handler_when_service_impl_supplied()
    {
        var svc = CreateService(Substitute.For<IReplicationApplier>(), out var method);
        LatticeReplicationGrpcMethodHolder.Current = method;

        var binder = Substitute.For<ServiceBinderBase>();
        Assert.That(
            () => LatticeReplicationGrpcServiceBase.BindService(binder, svc),
            Throws.Nothing);
    }

    // ---- Cross-cluster blocked-floor propagation -----------------------

    /// <summary>
    /// The gRPC <c>Push</c> handler must stamp the receiver-side
    /// blocked-floor pin (the lowest staged HLC across every partially
    /// buffered atomic batch on this tree) onto the <see cref="ReplicationAck.BlockedAtHlc"/>
    /// slot it returns to the producer. This is the cross-cluster
    /// propagation half of the TX-aware GC pin: the producer's
    /// shipper reads the slot off the ack and republishes it under
    /// its own consumer id so its WAL GC AND-s the same strict-less
    /// clause into its trim predicate.
    /// </summary>
    [Test]
    public async Task Push_stamps_receiver_side_blocked_floor_on_ack_from_registry()
    {
        var applier = Substitute.For<IReplicationApplier>();
        applier.ApplyBatchAsync(Arg.Any<IReadOnlyList<WalRecord>>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(new ApplyResult { Applied = true, HighWaterMark = HybridLogicalClock.Zero }));

        var registry = new InMemoryWalCursorRegistry();
        // Pre-pin a buffer floor on the receiver side as if a partial
        // atomic batch were staged. The HLC=Zero cursor uses the
        // blocked-floor overload that the applier publishes on every
        // admit -- exactly the shape the production code uses.
        var floor = new HybridLogicalClock { WallClockTicks = 12345, Counter = 0 };
        await registry.ReportCursorAsync("tree", "applier:atomic-batch", HybridLogicalClock.Zero, blockedAtHlc: floor);

        var svc = CreateService(applier, registry, out _);
        var box = new ReplicationBatchEnvelopeBox
        {
            Value = new ReplicationBatchEnvelope
            {
                TreeName = "tree",
                OriginClusterId = "remote",
                Entries = new[] { MakeSet("a", new HybridLogicalClock { WallClockTicks = 1, Counter = 0 }) },
            },
        };

        var ack = await svc.Push(box, new TestServerCallContext());

        Assert.Multiple(() =>
        {
            Assert.That(ack.Value.Accepted, Is.True);
            Assert.That(ack.Value.BlockedAtHlc, Is.Not.Null);
            Assert.That(ack.Value.BlockedAtHlc!.Value, Is.EqualTo(floor));
        });
    }

    /// <summary>
    /// When no consumer has registered a buffer pin
    /// the ack carries <see langword="null"/> in the
    /// <see cref="ReplicationAck.BlockedAtHlc"/> slot. The legacy
    /// HLC-only producer GC degrades cleanly in that case.
    /// </summary>
    [Test]
    public async Task Push_omits_blocked_floor_on_ack_when_no_pin_registered()
    {
        var applier = Substitute.For<IReplicationApplier>();
        applier.ApplyBatchAsync(Arg.Any<IReadOnlyList<WalRecord>>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(new ApplyResult { Applied = true, HighWaterMark = HybridLogicalClock.Zero }));

        var registry = new InMemoryWalCursorRegistry();
        // No reports issued.

        var svc = CreateService(applier, registry, out _);
        var box = new ReplicationBatchEnvelopeBox
        {
            Value = new ReplicationBatchEnvelope
            {
                TreeName = "tree",
                OriginClusterId = "remote",
                Entries = Array.Empty<WalRecord>(),
            },
        };

        var ack = await svc.Push(box, new TestServerCallContext());

        Assert.That(ack.Value.BlockedAtHlc, Is.Null);
    }

    /// <summary>
    /// A buffer pin registered against <c>tree-A</c>
    /// must not bleed into the ack returned for a push to
    /// <c>tree-B</c>. The server-side <c>GetBlockedFloorAsync</c>
    /// call is keyed on the request's <see cref="ReplicationBatchEnvelope.TreeName"/>,
    /// not on a process-wide singleton.
    /// </summary>
    [Test]
    public async Task Push_blocked_floor_is_isolated_per_tree()
    {
        var applier = Substitute.For<IReplicationApplier>();
        applier.ApplyBatchAsync(Arg.Any<IReadOnlyList<WalRecord>>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(new ApplyResult { Applied = true, HighWaterMark = HybridLogicalClock.Zero }));

        var registry = new InMemoryWalCursorRegistry();
        var floorA = new HybridLogicalClock { WallClockTicks = 500, Counter = 0 };
        await registry.ReportCursorAsync("tree-A", "applier:atomic-batch", HybridLogicalClock.Zero, blockedAtHlc: floorA);

        var svc = CreateService(applier, registry, out _);
        var box = new ReplicationBatchEnvelopeBox
        {
            Value = new ReplicationBatchEnvelope
            {
                TreeName = "tree-B",
                OriginClusterId = "remote",
                Entries = Array.Empty<WalRecord>(),
            },
        };

        var ack = await svc.Push(box, new TestServerCallContext());

        Assert.That(ack.Value.BlockedAtHlc, Is.Null,
            "ack for tree-B must not carry tree-A's pin");
    }

    // ---- Receiver-side flow control (R-062 surface) ---------------------

    /// <summary>
    /// Captures the <see cref="ReceiverFlowControlContext"/> the policy
    /// was invoked with, and returns a caller-supplied
    /// <see cref="ReceiverFlowControlHint"/> on every call.
    /// </summary>
    private sealed class CapturingFlowControlPolicy : IReceiverFlowControlPolicy
    {
        private readonly ReceiverFlowControlHint _hint;
        public ReceiverFlowControlContext? LastContext { get; private set; }
        public int Calls { get; private set; }

        public CapturingFlowControlPolicy(ReceiverFlowControlHint hint) => _hint = hint;

        public ValueTask<ReceiverFlowControlHint> EvaluateAsync(
            ReceiverFlowControlContext context,
            CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            LastContext = context;
            Calls++;
            return ValueTask.FromResult(_hint);
        }
    }

    private sealed class ThrowingFlowControlPolicy : IReceiverFlowControlPolicy
    {
        public ValueTask<ReceiverFlowControlHint> EvaluateAsync(
            ReceiverFlowControlContext context,
            CancellationToken cancellationToken)
            => throw new InvalidOperationException("policy boom");
    }

    [Test]
    public async Task Push_stamps_suggested_batch_size_from_policy_onto_ack()
    {
        var applier = Substitute.For<IReplicationApplier>();
        applier.ApplyBatchAsync(Arg.Any<IReadOnlyList<WalRecord>>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(new ApplyResult { Applied = true, HighWaterMark = HybridLogicalClock.Zero }));

        var policy = new CapturingFlowControlPolicy(new ReceiverFlowControlHint
        {
            SuggestedBatchSize = 16,
            PauseForMs = null,
        });

        var svc = CreateService(applier, new InMemoryWalCursorRegistry(), policy, out _);
        var box = new ReplicationBatchEnvelopeBox
        {
            Value = new ReplicationBatchEnvelope
            {
                TreeName = "tree",
                OriginClusterId = "remote",
                Entries = new[] { MakeSet("a", new HybridLogicalClock { WallClockTicks = 1, Counter = 0 }) },
            },
        };

        var ack = await svc.Push(box, new TestServerCallContext());

        Assert.Multiple(() =>
        {
            Assert.That(ack.Value.SuggestedBatchSize, Is.EqualTo(16));
            Assert.That(ack.Value.PauseForMs, Is.Null);
            Assert.That(policy.Calls, Is.EqualTo(1));
            Assert.That(policy.LastContext, Is.Not.Null);
            Assert.That(policy.LastContext!.Value.TreeName, Is.EqualTo("tree"));
            Assert.That(policy.LastContext!.Value.OriginClusterId, Is.EqualTo("remote"));
            Assert.That(policy.LastContext!.Value.EntryCount, Is.EqualTo(1));
        });
    }

    [Test]
    public async Task Push_stamps_pause_for_ms_from_policy_onto_ack()
    {
        var applier = Substitute.For<IReplicationApplier>();
        applier.ApplyBatchAsync(Arg.Any<IReadOnlyList<WalRecord>>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(new ApplyResult { Applied = true, HighWaterMark = HybridLogicalClock.Zero }));

        var policy = new CapturingFlowControlPolicy(new ReceiverFlowControlHint
        {
            SuggestedBatchSize = null,
            PauseForMs = 500,
        });

        var svc = CreateService(applier, new InMemoryWalCursorRegistry(), policy, out _);
        var box = new ReplicationBatchEnvelopeBox
        {
            Value = new ReplicationBatchEnvelope
            {
                TreeName = "tree",
                OriginClusterId = "remote",
                Entries = Array.Empty<WalRecord>(),
            },
        };

        var ack = await svc.Push(box, new TestServerCallContext());

        Assert.Multiple(() =>
        {
            Assert.That(ack.Value.SuggestedBatchSize, Is.Null);
            Assert.That(ack.Value.PauseForMs, Is.EqualTo(500));
        });
    }

    [Test]
    public async Task Push_default_policy_returns_null_hint_slots()
    {
        var applier = Substitute.For<IReplicationApplier>();
        applier.ApplyBatchAsync(Arg.Any<IReadOnlyList<WalRecord>>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(new ApplyResult { Applied = true, HighWaterMark = HybridLogicalClock.Zero }));

        // CreateService overloads default to NoOpReceiverFlowControlPolicy.Instance.
        var svc = CreateService(applier, out _);
        var box = new ReplicationBatchEnvelopeBox
        {
            Value = new ReplicationBatchEnvelope
            {
                TreeName = "tree",
                OriginClusterId = "remote",
                Entries = Array.Empty<WalRecord>(),
            },
        };

        var ack = await svc.Push(box, new TestServerCallContext());

        Assert.Multiple(() =>
        {
            Assert.That(ack.Value.SuggestedBatchSize, Is.Null);
            Assert.That(ack.Value.PauseForMs, Is.Null);
        });
    }

    [Test]
    public async Task Push_swallows_policy_failure_and_returns_null_hint_slots()
    {
        var applier = Substitute.For<IReplicationApplier>();
        applier.ApplyBatchAsync(Arg.Any<IReadOnlyList<WalRecord>>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(new ApplyResult { Applied = true, HighWaterMark = HybridLogicalClock.Zero }));

        var svc = CreateService(applier, new InMemoryWalCursorRegistry(), new ThrowingFlowControlPolicy(), out _);
        var box = new ReplicationBatchEnvelopeBox
        {
            Value = new ReplicationBatchEnvelope
            {
                TreeName = "tree",
                OriginClusterId = "remote",
                Entries = Array.Empty<WalRecord>(),
            },
        };

        var ack = await svc.Push(box, new TestServerCallContext());

        Assert.Multiple(() =>
        {
            Assert.That(ack.Value.Accepted, Is.True);
            Assert.That(ack.Value.SuggestedBatchSize, Is.Null);
            Assert.That(ack.Value.PauseForMs, Is.Null);
        });
    }
}






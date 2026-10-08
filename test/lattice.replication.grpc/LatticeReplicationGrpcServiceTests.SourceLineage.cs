using Grpc.Core;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Replication.Grpc.Tests;

/// <summary>
/// Issue #4707: the source-lineage check of a push (issue #4673) runs at the
/// applier's admission seam, so the gRPC call site has exactly two duties - hand
/// the sender's stamp to the applier on the lineage scope, and answer a
/// lineage-refused result with a not-accepted ack that carries
/// <see cref="ReplicationAck.SourceLineageRefused"/>. Each test fails when the
/// service drops one of them.
/// </summary>
public partial class LatticeReplicationGrpcServiceTests
{
    private const string LineageSender = "remote";

    /// <summary>
    /// Records the lineage stamp in force when the service calls the applier,
    /// and answers with <see cref="Result"/>.
    /// </summary>
    private sealed class LineageRecordingApplier : IReplicationApplier
    {
        public ApplyResult Result { get; init; } = new() { Applied = true, HighWaterMark = HybridLogicalClock.Zero };

        public bool Called { get; private set; }

        public ReplicationSourceLineageStamp? SeenStamp { get; private set; }

        public string? SeenAuthenticatedSender { get; private set; }

        public Guid? SeenFrontierEpoch { get; private set; }

        public Task<ApplyResult> ApplyAsync(WalRecord entry, CancellationToken cancellationToken = default) =>
            ApplyBatchAsync([entry], cancellationToken);

        public Task<ApplyResult> ApplyBatchAsync(IReadOnlyList<WalRecord> entries, CancellationToken cancellationToken = default)
        {
            Called = true;
            SeenStamp = ReplicationSourceLineageScope.Current;
            SeenAuthenticatedSender = ReplicationSourceLineageScope.CurrentAuthenticatedSenderClusterId;
            SeenFrontierEpoch = ReplicationSourceLineageScope.Active?.ObservedFrontierEpoch;
            return Task.FromResult(Result);
        }
    }

    /// <summary>A conforming peer's call context that also carries the source lineage header.</summary>
    private sealed class LineageCallContext(string? sourceLineage) : ServerCallContext
    {
        protected override string MethodCore => "Push";
        protected override string HostCore => string.Empty;
        protected override string PeerCore => string.Empty;
        protected override DateTime DeadlineCore => DateTime.MaxValue;
        protected override global::Grpc.Core.Metadata RequestHeadersCore
        {
            get
            {
                var headers = new global::Grpc.Core.Metadata
                {
                    { LatticeReplicationGrpcMetadataNames.OriginClusterIdHeader, LineageSender },
                };
                if (sourceLineage is not null)
                {
                    headers.Add(LatticeReplicationGrpcMetadataNames.SourceLineageHeader, sourceLineage);
                }

                return headers;
            }
        }
        protected override CancellationToken CancellationTokenCore => CancellationToken.None;
        protected override global::Grpc.Core.Metadata ResponseTrailersCore => new();
        protected override Status StatusCore { get; set; }
        protected override WriteOptions? WriteOptionsCore { get; set; }
        protected override AuthContext AuthContextCore => null!;
        protected override ContextPropagationToken CreatePropagationTokenCore(ContextPropagationOptions? options) => null!;
        protected override Task WriteResponseHeadersAsyncCore(global::Grpc.Core.Metadata responseHeaders) => Task.CompletedTask;
    }

    private static ReplicationBatchEnvelopeBox LineageBox() => new()
    {
        Value = new ReplicationBatchEnvelope
        {
            TreeName = "tree",
            OriginClusterId = LineageSender,
            Entries = [MakeSet("k", new HybridLogicalClock { WallClockTicks = 7 }), MakeSet("j", new HybridLogicalClock { WallClockTicks = 8 })],
        },
    };

    [Test]
    public async Task Push_hands_the_sender_stamped_lineage_to_the_applier_seam()
    {
        var lineage = Guid.NewGuid();
        var applier = new LineageRecordingApplier();
        var svc = CreateService(applier, out _);

        var ack = await svc.Push(LineageBox(), new LineageCallContext(lineage.ToString("D")));

        Assert.Multiple(() =>
        {
            Assert.That(applier.Called, Is.True, "precondition: the batch reached the applier");
            Assert.That(applier.SeenAuthenticatedSender, Is.EqualTo(LineageSender));
            Assert.That(applier.SeenStamp, Is.EqualTo(new ReplicationSourceLineageStamp(LineageSender, lineage)),
                "the applier checks the batch against the lineage the authenticated sender stamped, so the service must pass it");
            Assert.That(applier.SeenFrontierEpoch, Is.EqualTo(ack.Value.ReceiverLineage),
                "the check uses the frontier epoch the ack reports");
            Assert.That(ack.Value.Accepted, Is.True);
            Assert.That(ack.Value.SourceLineageRefused, Is.False);
        });
    }

    [Test]
    public async Task Push_hands_a_malformed_lineage_header_to_the_applier_as_a_lineage_that_matches_nothing()
    {
        var applier = new LineageRecordingApplier();
        var svc = CreateService(applier, out _);

        await svc.Push(LineageBox(), new LineageCallContext("not-a-guid"));

        Assert.That(applier.SeenStamp, Is.EqualTo(new ReplicationSourceLineageStamp(LineageSender, Guid.Empty)),
            "a malformed header vouches for no lineage, so it is refused like a mismatch rather than read as unstamped");
    }

    [Test]
    public async Task Push_without_a_lineage_header_applies_unstamped()
    {
        var applier = new LineageRecordingApplier();
        var svc = CreateService(applier, out _);

        await svc.Push(LineageBox(), new LineageCallContext(null));

        Assert.Multiple(() =>
        {
            Assert.That(applier.Called, Is.True);
            Assert.That(applier.SeenAuthenticatedSender, Is.EqualTo(LineageSender),
                "source authorization uses the authenticated sender even without a lineage header");
            Assert.That(applier.SeenStamp, Is.Null, "a sender that predates the stamp is not checked");
        });
    }

    [Test]
    public async Task Push_answers_a_lineage_refused_apply_with_a_not_accepted_lineage_refused_ack()
    {
        var applier = new LineageRecordingApplier
        {
            Result = new ApplyResult
            {
                Applied = false,
                HighWaterMark = HybridLogicalClock.Zero,
                SourceLineageRefused = true,
            },
        };
        var svc = CreateService(applier, out _);

        var ack = await svc.Push(LineageBox(), new LineageCallContext(Guid.NewGuid().ToString("D")));

        Assert.Multiple(() =>
        {
            Assert.That(ack.Value.Accepted, Is.False,
                "an accepted ack would move the sender's cursor past a batch that was never applied");
            Assert.That(ack.Value.SourceLineageRefused, Is.True,
                "the sender re-resolves its binding only when told the lineage was refused");
            Assert.That(ack.Value.HighestAppliedHlc, Is.EqualTo(HybridLogicalClock.Zero));
        });
    }
}

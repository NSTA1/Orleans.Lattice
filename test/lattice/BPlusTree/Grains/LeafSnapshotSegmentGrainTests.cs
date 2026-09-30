using NSubstitute;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Unit tests for <see cref="LeafSnapshotSegmentGrain"/>, the one-read/one-write/
/// one-clear holder for a single persisted snapshot segment.
/// <para>
/// <b>Both fail-closed contracts here are load-bearing, and both fail silently if
/// they regress.</b> A segment is written <i>before</i> the manifest that commits
/// it, so a frame accepted on the write path that does not decode would be
/// referenced by a manifest claiming coverage the snapshot cannot reproduce. And a
/// frame returned on the read path whose row count disagrees with what the writer
/// recorded would hand the caller a snapshot with silently fewer rows - after
/// which the coverage-gated WAL GC has trimmed a prefix nothing can reproduce.
/// Neither failure announces itself; both present as a snapshot that restores
/// cleanly and is short.
/// </para>
/// </summary>
[TestFixture]
public sealed class LeafSnapshotSegmentGrainTests
{
    private static (LeafSnapshotSegmentGrain Grain, FakePersistentState<LeafSnapshotSegment> State)
        CreateGrain(FakePersistentState<LeafSnapshotSegment>? state = null)
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(
            GrainId.Create("leaf-snapshot-segment", Guid.NewGuid().ToString("N")));
        state ??= new FakePersistentState<LeafSnapshotSegment>();
        return (new LeafSnapshotSegmentGrain(context, state), state);
    }

    private static LeafSnapshotRow[] Rows(int count)
    {
        var rows = new LeafSnapshotRow[count];
        for (var i = 0; i < count; i++)
        {
            // A per-row fill, so a fold that duplicated or reordered rows is
            // detectable by value rather than only by count.
            var payload = new byte[8];
            Array.Fill(payload, (byte)(i % 251));
            rows[i] = new LeafSnapshotRow(
                $"segment-key-{i:D4}",
                LwwValue<byte[]>.Create(
                    payload, new HybridLogicalClock { WallClockTicks = 1_000L + i }));
        }

        return rows;
    }

    private static byte[] Frame(int rowCount = 3) => LeafSnapshotCodec.Encode(Rows(rowCount));

    private static CancellationToken Cancelled()
    {
        using var cts = new CancellationTokenSource();
        cts.Cancel();
        return cts.Token;
    }

    [Test]
    public void SaveAsync_rejects_a_null_frame()
    {
        var (grain, _) = CreateGrain();

        Assert.That(
            async () => await grain.SaveAsync(null!, rowCount: 1, CancellationToken.None),
            Throws.ArgumentNullException);
    }

    /// <summary>
    /// An empty frame is refused rather than persisted. Persisting it would let the
    /// manifest commit a segment that decodes to nothing.
    /// </summary>
    [Test]
    public void SaveAsync_refuses_an_empty_frame_without_writing()
    {
        var (grain, state) = CreateGrain();

        Assert.That(
            async () => await grain.SaveAsync([], rowCount: 0, CancellationToken.None),
            Throws.ArgumentException);
        Assert.Multiple(() =>
        {
            Assert.That(state.WriteCount, Is.Zero, "a refused frame must not reach the provider");
            Assert.That(state.State.Frame, Is.Null);
        });
    }

    /// <summary>
    /// The refusal that matters most: a frame that is not empty and does not decode.
    /// Accepting it would be referenced by a manifest reporting coverage the snapshot
    /// cannot reproduce, and nothing downstream re-checks.
    /// </summary>
    [Test]
    public void SaveAsync_refuses_a_frame_that_does_not_decode_without_writing()
    {
        var (grain, state) = CreateGrain();
        var garbage = new byte[] { 0xDE, 0xAD, 0xBE, 0xEF, 0x01, 0x02, 0x03, 0x04 };
        Assert.That(
            LeafSnapshotCodec.Validate(garbage),
            Is.False,
            "precondition: the fixture's input really is undecodable");

        Assert.That(
            async () => await grain.SaveAsync(garbage, rowCount: 1, CancellationToken.None),
            Throws.ArgumentException);
        Assert.Multiple(() =>
        {
            Assert.That(state.WriteCount, Is.Zero);
            Assert.That(state.State.Frame, Is.Null);
        });
    }

    [Test]
    public void SaveAsync_observes_a_cancelled_token_before_validating()
    {
        var (grain, state) = CreateGrain();

        Assert.That(
            async () => await grain.SaveAsync(Frame(), rowCount: 3, Cancelled()),
            Throws.InstanceOf<OperationCanceledException>());
        Assert.That(state.WriteCount, Is.Zero);
    }

    [Test]
    public async Task SaveAsync_persists_the_frame_and_its_row_count()
    {
        var (grain, state) = CreateGrain();
        var frame = Frame(rowCount: 5);

        await grain.SaveAsync(frame, rowCount: 5, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(state.State.Frame, Is.EqualTo(frame));
            Assert.That(state.State.RowCount, Is.EqualTo(5));
            Assert.That(
                state.WriteCount,
                Is.EqualTo(1),
                "the write must land before the manifest that commits it is written");
        });
    }

    [Test]
    public async Task SaveAsync_overwrites_a_previous_frame()
    {
        var (grain, state) = CreateGrain();
        await grain.SaveAsync(Frame(rowCount: 2), rowCount: 2, CancellationToken.None);

        var replacement = Frame(rowCount: 7);
        await grain.SaveAsync(replacement, rowCount: 7, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(state.State.Frame, Is.EqualTo(replacement));
            Assert.That(state.State.RowCount, Is.EqualTo(7));
            Assert.That(state.WriteCount, Is.EqualTo(2));
        });
    }

    [Test]
    public void LoadFrameAsync_observes_a_cancelled_token()
    {
        var (grain, _) = CreateGrain();

        Assert.That(
            async () => await grain.LoadFrameAsync(Cancelled()),
            Throws.InstanceOf<OperationCanceledException>());
    }

    [Test]
    public async Task LoadFrameAsync_returns_null_when_no_segment_has_been_written()
    {
        var (grain, _) = CreateGrain();

        Assert.That(await grain.LoadFrameAsync(CancellationToken.None), Is.Null);
    }

    /// <summary>
    /// A zero-length frame reads as absent rather than as an empty segment, which is
    /// the same reading the post-clear shape produces.
    /// </summary>
    [Test]
    public async Task LoadFrameAsync_reads_a_zero_length_frame_as_absent()
    {
        var (grain, state) = CreateGrain();
        state.State.Frame = [];

        Assert.That(await grain.LoadFrameAsync(CancellationToken.None), Is.Null);
    }

    [Test]
    public async Task LoadFrameAsync_round_trips_a_saved_frame_by_reference()
    {
        var (grain, _) = CreateGrain();
        var frame = Frame(rowCount: 4);
        await grain.SaveAsync(frame, rowCount: 4, CancellationToken.None);

        var loaded = await grain.LoadFrameAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(loaded, Is.EqualTo(frame));
            Assert.That(
                ReferenceEquals(loaded, frame),
                Is.True,
                "Frame is [Immutable] so a same-silo read shares the array rather than "
                + "allocating a second window-sized copy");
        });
    }

    /// <summary>
    /// A stored frame that no longer decodes is reported as absent, which the caller
    /// turns into "this segmented snapshot is not usable" and falls back to WAL
    /// replay. Returning it would hand the caller silently fewer rows.
    /// </summary>
    [Test]
    public async Task LoadFrameAsync_reads_a_corrupted_frame_as_absent()
    {
        var (grain, state) = CreateGrain();
        var frame = Frame(rowCount: 4);
        await grain.SaveAsync(frame, rowCount: 4, CancellationToken.None);

        var corrupted = (byte[])frame.Clone();
        corrupted[^1] ^= 0xFF;
        state.State.Frame = corrupted;
        Assert.That(
            LeafSnapshotCodec.Validate(corrupted),
            Is.False,
            "precondition: the corruption really did break the frame");

        Assert.That(await grain.LoadFrameAsync(CancellationToken.None), Is.Null);
    }

    /// <summary>
    /// The cross-check that turns a silently truncated segment into a detected one:
    /// a frame that decodes perfectly but carries a different number of rows from
    /// what the writer recorded is still refused.
    /// </summary>
    [Test]
    public async Task LoadFrameAsync_refuses_a_frame_whose_row_count_disagrees_with_the_writers()
    {
        var (grain, state) = CreateGrain();
        var frame = Frame(rowCount: 4);
        await grain.SaveAsync(frame, rowCount: 4, CancellationToken.None);

        // The frame is untouched and decodes cleanly; only the recorded intent
        // disagrees, which is exactly the shape a truncated write leaves behind.
        state.State.RowCount = 9;

        Assert.Multiple(async () =>
        {
            Assert.That(LeafSnapshotCodec.Validate(state.State.Frame), Is.True);
            Assert.That(await grain.LoadFrameAsync(CancellationToken.None), Is.Null);
        });
    }

    [Test]
    public void ClearAsync_observes_a_cancelled_token()
    {
        var (grain, _) = CreateGrain();

        Assert.That(
            async () => await grain.ClearAsync(Cancelled()),
            Throws.InstanceOf<OperationCanceledException>());
    }

    /// <summary>
    /// Clearing a segment that holds nothing is I/O-free: a real provider's clear
    /// deletes the underlying row, so short-circuiting keeps an idempotent clear
    /// from touching storage at all.
    /// </summary>
    [Test]
    public async Task ClearAsync_short_circuits_when_there_is_nothing_to_clear()
    {
        var (grain, state) = CreateGrain();

        await grain.ClearAsync(CancellationToken.None);

        Assert.That(
            state.RecordExists,
            Is.True,
            "the provider was never called, so the row was never deleted");
    }

    [Test]
    public async Task ClearAsync_drops_a_persisted_segment()
    {
        var (grain, state) = CreateGrain();
        await grain.SaveAsync(Frame(rowCount: 3), rowCount: 3, CancellationToken.None);

        await grain.ClearAsync(CancellationToken.None);

        Assert.Multiple(async () =>
        {
            Assert.That(state.State.Frame, Is.Null);
            Assert.That(state.State.RowCount, Is.Zero);
            Assert.That(state.RecordExists, Is.False, "a real clear deletes the row");
            Assert.That(await grain.LoadFrameAsync(CancellationToken.None), Is.Null);
        });
    }

    [Test]
    public async Task ClearAsync_is_idempotent()
    {
        var (grain, state) = CreateGrain();
        await grain.SaveAsync(Frame(rowCount: 3), rowCount: 3, CancellationToken.None);

        await grain.ClearAsync(CancellationToken.None);
        await grain.ClearAsync(CancellationToken.None);

        Assert.That(state.State.Frame, Is.Null);
    }

    [Test]
    public async Task A_cleared_segment_can_be_written_again()
    {
        var (grain, _) = CreateGrain();
        await grain.SaveAsync(Frame(rowCount: 2), rowCount: 2, CancellationToken.None);
        await grain.ClearAsync(CancellationToken.None);

        var reused = Frame(rowCount: 6);
        await grain.SaveAsync(reused, rowCount: 6, CancellationToken.None);

        Assert.That(await grain.LoadFrameAsync(CancellationToken.None), Is.EqualTo(reused));
    }

    /// <summary>
    /// The grain exposes its activation context, which is how the Orleans runtime
    /// resolves the activation this segment belongs to.
    /// </summary>
    [Test]
    public void The_grain_exposes_its_activation_context()
    {
        var context = Substitute.For<IGrainContext>();
        var id = GrainId.Create("leaf-snapshot-segment", "segment-7");
        context.GrainId.Returns(id);
        var grain = new LeafSnapshotSegmentGrain(
            context, new FakePersistentState<LeafSnapshotSegment>());

        Assert.That(((IGrainBase)grain).GrainContext, Is.SameAs(context));
    }
}

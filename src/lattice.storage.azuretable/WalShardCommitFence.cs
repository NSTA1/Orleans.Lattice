using Azure;

namespace Orleans.Lattice.Storage.AzureTable;

/// <summary>
/// The shard's commit fence on one provider instance (issue #5810): the <c>TAIL</c>
/// row's ETag as this instance last knew it, and a generation that refuses claims
/// made before a write this instance did not make.
/// </summary>
/// <remarks>
/// <para>
/// Entry rows are create-only per batch partition, keyed by the batch's start offset,
/// and the overlap guard sees only this instance's batches. A writer that cannot see
/// another (a predecessor activation still running on another silo, after its WAL
/// shard moved) can therefore land a batch at a different start over offsets the
/// other already committed, and its blind <c>TAIL</c> upsert can lower <c>TAIL</c>.
/// </para>
/// <para>
/// Every phase-2 transaction therefore writes <c>TAIL</c> conditionally on the ETag
/// held here, never lowering it, so commits on the shard are totally ordered: a
/// commit lands only if no other writer moved <c>TAIL</c> since this instance last
/// observed or wrote it. A refused commit, or any failed one, invalidates the fence:
/// the generation advances, so every batch claimed under the old one is refused
/// without being submitted, and the ETag is forgotten until the next read of
/// <c>TAIL</c> fills it again. No claim is admitted while the ETag is unknown.
/// </para>
/// <para>
/// Nothing here costs a round trip: the ETag is filled by reads the provider already
/// makes (the overlap guard's bound read, <c>GetHighestOffsetAsync</c>) and replaced
/// from each successful commit's own response.
/// </para>
/// <para>
/// The fence and the shard's <see cref="WalShardOverlapGuard"/> hold one invariant
/// together: while the fence holds an ETag and the guard is bounded, the guard's bound
/// is at least the <c>TAIL</c> value that ETag was observed with. A claim the guard
/// admits under a generation therefore starts above everything committed when the
/// ETag was observed, and the conditional write proves nothing was committed since.
/// The guard's bound is raised or reset only through this type, under its lock, which
/// is always taken before the guard's.
/// </para>
/// </remarks>
internal sealed class WalShardCommitFence
{
    private readonly object _gate = new();
    private long _generation;
    private bool _known;
    private ETag? _tailETag;
    private long _tail = -1L;

    /// <summary>
    /// Returns whether the fence holds an ETag, and the generation a claim made now carries.
    /// </summary>
    internal bool TryGetGeneration(out long generation)
    {
        lock (_gate)
        {
            generation = _generation;
            return _known;
        }
    }

    /// <summary>
    /// The synchronous fast path: claims <c>[start, endInclusive]</c> on
    /// <paramref name="guard"/> when the fence holds an ETag and the batch starts above
    /// every batch that may be written, capturing the generation in the same step.
    /// Returns <see langword="false"/> without claiming anything otherwise.
    /// </summary>
    internal bool TryClaimAboveWritten(WalShardOverlapGuard guard, long start, long endInclusive, out long generation)
    {
        lock (_gate)
        {
            generation = _generation;
            return _known && guard.TryClaimAboveWritten(start, endInclusive);
        }
    }

    /// <summary>
    /// Fills the fence from a read of <c>TAIL</c> when it holds no ETag, raises the
    /// guard's bound to cover it, and returns the generation a claim made now carries.
    /// A held ETag is never replaced by a read: a read can race this instance's own
    /// commit, and only a refused commit may tell the two apart.
    /// </summary>
    /// <param name="guard">The shard's overlap guard.</param>
    /// <param name="tailETag">The row's ETag, or <see langword="null"/> when the row is absent.</param>
    /// <param name="tail">The row's offset, or -1 when it is absent.</param>
    /// <param name="writtenUpperBound">
    /// An upper bound on every written batch read with <paramref name="tail"/>, which
    /// bounds the guard; <see langword="null"/> for a read of <c>TAIL</c> alone, which
    /// only raises a guard already bounded.
    /// </param>
    internal long Fill(WalShardOverlapGuard guard, ETag? tailETag, long tail, long? writtenUpperBound)
    {
        lock (_gate)
        {
            if (!_known)
            {
                _known = true;
                _tailETag = tailETag;
                _tail = tail;
            }

            if (writtenUpperBound is { } bound)
            {
                guard.RaiseBound(Math.Max(bound, _tail));
            }
            else if (guard.IsBounded)
            {
                guard.RaiseBound(_tail);
            }

            return _generation;
        }
    }

    /// <summary>
    /// The condition a commit of claims made under <paramref name="generation"/> must
    /// write <c>TAIL</c> under, or <see langword="false"/> when those claims are fenced.
    /// </summary>
    /// <param name="generation">The generation the commit's claims carry.</param>
    /// <param name="tailETag">The ETag to match, or <see langword="null"/> to create an absent row.</param>
    /// <param name="tail">The <c>TAIL</c> value the ETag was observed with; a commit never writes below it.</param>
    internal bool TryGetCondition(long generation, out ETag? tailETag, out long tail)
    {
        lock (_gate)
        {
            tailETag = _tailETag;
            tail = _tail;
            return _known && generation == _generation;
        }
    }

    /// <summary>Records the ETag and value a successful commit of <paramref name="generation"/> wrote.</summary>
    internal void Advance(long generation, ETag? tailETag, long tail)
    {
        lock (_gate)
        {
            if (generation != _generation || !_known)
            {
                return;
            }

            if (tailETag is null)
            {
                // A response without the row's ETag cannot condition the next commit.
                _generation++;
                _known = false;
                return;
            }

            _tailETag = tailETag;
            _tail = tail;
        }
    }

    /// <summary>
    /// Fences every claim made so far: the generation advances and the ETag is
    /// forgotten. Called on any failed or abandoned commit, a refusal included.
    /// </summary>
    internal void Invalidate()
    {
        lock (_gate)
        {
            _generation++;
            _known = false;
            _tailETag = null;
        }
    }

    /// <summary>
    /// Fences every claim made so far and resets the guard after reconciliation, which
    /// may itself have written <c>TAIL</c>; see <see cref="WalShardOverlapGuard.Reset"/>.
    /// </summary>
    internal void Reset(WalShardOverlapGuard guard, long? writtenUpperBound)
    {
        lock (_gate)
        {
            _generation++;
            _known = false;
            _tailETag = null;
            guard.Reset(writtenUpperBound);
        }
    }
}

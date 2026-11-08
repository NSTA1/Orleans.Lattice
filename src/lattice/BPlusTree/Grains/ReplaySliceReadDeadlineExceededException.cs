using System;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// A starvation drive's slice read did not answer within its per-read deadline
/// (issue #6007), so the drive gives it up before the cluster response timeout
/// would surface it as a raw <see cref="TimeoutException"/>.
/// </summary>
/// <remarks>
/// Internal and never serialized: it is raised and caught inside one drive and
/// must never escape a grain method. It derives directly from
/// <see cref="Exception"/>, not from <see cref="OperationCanceledException"/>
/// or <see cref="TimeoutException"/>, so no catch filter written for those can
/// absorb it on the way out of the replay.
/// </remarks>
internal sealed class ReplaySliceReadDeadlineExceededException : Exception
{
    /// <summary>Creates the fault for one unanswered slice read.</summary>
    /// <param name="treeId">The tree being replayed.</param>
    /// <param name="partition">The WAL partition being read.</param>
    /// <param name="fromExclusive">The exclusive lower bound of the read.</param>
    /// <param name="deadline">The per-read deadline that expired.</param>
    public ReplaySliceReadDeadlineExceededException(string treeId, int partition, long fromExclusive, TimeSpan deadline)
        : base($"A replay slice read of tree '{treeId}' partition {partition} from offset {fromExclusive} did not answer within its {deadline} deadline.")
    {
        TreeId = treeId;
        Partition = partition;
        FromExclusive = fromExclusive;
        Deadline = deadline;
    }

    /// <summary>The tree being replayed.</summary>
    public string TreeId { get; }

    /// <summary>The WAL partition being read.</summary>
    public int Partition { get; }

    /// <summary>The exclusive lower bound of the read.</summary>
    public long FromExclusive { get; }

    /// <summary>The per-read deadline that expired.</summary>
    public TimeSpan Deadline { get; }
}

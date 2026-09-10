namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// Raised when <see cref="RepoContextVectorSource.CountAsync"/> stops short of the
/// end of the vector prefix because its wall-clock budget ran out, so the figure it
/// would otherwise return is a partial walk rather than a count.
/// </summary>
/// <remarks>
/// <para>
/// <b>Why this is a fault and not a smaller number.</b> The count is consumed as a
/// bound, and the two consumers need it bounded in opposite-looking but consistent
/// ways: the shortfall probe in <c>RepoContextAnnIndexHandle.CatchUpAsync</c> asks
/// whether the persisted index is BEHIND the store of record, and the build's
/// reservation asks roughly how large the corpus is. Over-counting makes the probe
/// repair when it need not, which is safe. UNDER-counting makes it skip a repair it
/// needed, which is not: the index stays quietly behind the store with nothing
/// reporting it. A truncated walk is precisely an under-count, so returning it would
/// convert a bounded walk into a silent correctness defect - a strictly worse bug
/// than the unbounded walk this budget exists to prevent (issue #2447).
/// </para>
/// <para>
/// Raising instead keeps "I could not count" distinguishable from "I counted, and it
/// is small", which is the only distinction either consumer actually needs. Both
/// treat the fault as "unknown", and both already resolve unknown in their own safe
/// direction: the probe repairs, and the build proceeds without a capacity
/// reservation. That mirrors the existing handling of a reconnect-budget exhaustion
/// on the same walk (#1844), which reached the same conclusion for the same reason.
/// </para>
/// <para>
/// It is deliberately NOT an <c>EnumerationAbortedException</c>. That exception is
/// Orleans' own, and it means a remote enumerator was reclaimed mid-scan - a
/// transient fault of the store. This is the caller declining to spend more time,
/// which is a local policy decision, and conflating the two would mislead whoever
/// next reads a log line or a catch clause. It is plain, internal, and never crosses
/// a grain boundary, so it needs no serializer, alias, or deep-copier.
/// </para>
/// </remarks>
internal sealed class RepoContextCountBudgetExceededException : Exception
{
    /// <summary>Creates the exception with the default message.</summary>
    public RepoContextCountBudgetExceededException()
        : base("The repository-context vector count did not reach the end of the prefix within its wall-clock budget.")
    {
    }

    /// <summary>Creates the exception describing the budget that was spent.</summary>
    /// <param name="repoId">The repository whose vector prefix was being counted.</param>
    /// <param name="counted">The number of keys walked before the budget ran out.</param>
    /// <param name="budget">The wall-clock budget that was exhausted.</param>
    public RepoContextCountBudgetExceededException(string repoId, int counted, TimeSpan budget)
        : base($"Counting the vector prefix for repository '{repoId}' walked {counted} keys without reaching the end "
            + $"of the prefix within its {budget.TotalSeconds:0.###}s budget, so the figure is a partial walk and is "
            + "reported as unknown rather than as a count.")
    {
        RepoId = repoId;
        Counted = counted;
        Budget = budget;
    }

    /// <summary>Creates the exception with a caller-supplied message.</summary>
    /// <param name="message">The message.</param>
    public RepoContextCountBudgetExceededException(string message)
        : base(message)
    {
    }

    /// <summary>Creates the exception with a caller-supplied message and cause.</summary>
    /// <param name="message">The message.</param>
    /// <param name="innerException">The cause.</param>
    public RepoContextCountBudgetExceededException(string message, Exception innerException)
        : base(message, innerException)
    {
    }

    /// <summary>
    /// The repository whose vector prefix was being counted, or <see langword="null"/>
    /// when the exception was not raised with that detail.
    /// </summary>
    public string? RepoId { get; }

    /// <summary>
    /// The number of keys walked before the budget ran out. This is a lower bound on
    /// the true count and is carried for diagnostics only - it is deliberately not
    /// offered to callers as a count, because consuming it as one is the under-count
    /// this exception exists to prevent.
    /// </summary>
    public int Counted { get; }

    /// <summary>The wall-clock budget that was exhausted.</summary>
    public TimeSpan Budget { get; }
}

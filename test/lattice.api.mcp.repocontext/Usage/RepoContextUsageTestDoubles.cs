namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests;

/// <summary>
/// A no-op <see cref="IRepoContextUsageRecorder"/> for tests that exercise the bundle service without
/// asserting on usage recording. It records nothing and summarises to an empty aggregate.
/// </summary>
internal sealed class NoOpUsageRecorder : IRepoContextUsageRecorder
{
    /// <summary>The shared singleton instance.</summary>
    public static readonly NoOpUsageRecorder Instance = new();

    private NoOpUsageRecorder()
    {
    }

    /// <inheritdoc />
    public TimeSpan Window => TimeSpan.FromHours(1);

    /// <inheritdoc />
    public void Record(in RepoContextCallUsage usage)
    {
    }

    /// <inheritdoc />
    public RepoContextUsageAggregate Summarize() => default;
}

/// <summary>
/// A capturing <see cref="IRepoContextUsageRecorder"/> that records every usage into an inspectable list,
/// so a test can assert exactly what figures a call recorded without any timing or windowing behaviour.
/// </summary>
internal sealed class CapturingUsageRecorder : IRepoContextUsageRecorder
{
    /// <summary>The figures recorded so far, in call order.</summary>
    public List<RepoContextCallUsage> Recorded { get; } = [];

    /// <inheritdoc />
    public TimeSpan Window => TimeSpan.FromHours(1);

    /// <inheritdoc />
    public void Record(in RepoContextCallUsage usage) => Recorded.Add(usage);

    /// <inheritdoc />
    public RepoContextUsageAggregate Summarize()
    {
        long response = 0, replaced = 0;
        foreach (var usage in Recorded)
        {
            response += usage.ResponseTokens;
            replaced += usage.ReplacedReadTokens;
        }

        return new RepoContextUsageAggregate(Recorded.Count, response, replaced);
    }
}

/// <summary>
/// A deterministic <see cref="TimeProvider"/> whose current time is set explicitly, so the recorder's
/// window can be advanced without any wall clock, timer, or <c>Task.Delay</c>.
/// </summary>
internal sealed class SettableTimeProvider : TimeProvider
{
    /// <summary>The current time this provider reports.</summary>
    public DateTimeOffset UtcNow { get; set; } = DateTimeOffset.UnixEpoch;

    /// <summary>Advances the current time by <paramref name="delta"/>.</summary>
    /// <param name="delta">The amount to advance by.</param>
    public void Advance(TimeSpan delta) => UtcNow += delta;

    /// <inheritdoc />
    public override DateTimeOffset GetUtcNow() => UtcNow;
}

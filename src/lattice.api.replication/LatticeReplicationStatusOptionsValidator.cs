using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Api.Replication;

/// <summary>
/// Validates <see cref="LatticeReplicationStatusOptions"/>: every configured bound
/// is non-negative, and a lagging bound never exceeds its stalled bound when both
/// are set. A misordered pair would make <see cref="ReplicationLinkHealth.Lagging"/>
/// unreachable for that signal, so it is rejected rather than silently tolerated.
/// </summary>
internal sealed class LatticeReplicationStatusOptionsValidator : IValidateOptions<LatticeReplicationStatusOptions>
{
    /// <inheritdoc />
    public ValidateOptionsResult Validate(string? name, LatticeReplicationStatusOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);

        var failures = new List<string>();
        CheckPair(
            failures,
            nameof(options.LaggingEntriesBehind), options.LaggingEntriesBehind,
            nameof(options.StalledEntriesBehind), options.StalledEntriesBehind);
        CheckPair(
            failures,
            nameof(options.LaggingConsecutiveErrors), options.LaggingConsecutiveErrors,
            nameof(options.StalledConsecutiveErrors), options.StalledConsecutiveErrors);
        CheckPair(
            failures,
            nameof(options.LaggingAfterNoContact), options.LaggingAfterNoContact?.Ticks,
            nameof(options.StalledAfterNoContact), options.StalledAfterNoContact?.Ticks);
        CheckPair(
            failures,
            nameof(options.InboundLaggingAfterNoContact), options.InboundLaggingAfterNoContact?.Ticks,
            nameof(options.InboundStalledAfterNoContact), options.InboundStalledAfterNoContact?.Ticks);

        return failures.Count == 0
            ? ValidateOptionsResult.Success
            : ValidateOptionsResult.Fail(failures);
    }

    private static void CheckPair(
        List<string> failures,
        string laggingName,
        long? lagging,
        string stalledName,
        long? stalled)
    {
        if (lagging < 0)
        {
            failures.Add($"{nameof(LatticeReplicationStatusOptions)}.{laggingName} must not be negative.");
        }

        if (stalled < 0)
        {
            failures.Add($"{nameof(LatticeReplicationStatusOptions)}.{stalledName} must not be negative.");
        }

        if (lagging is { } l && stalled is { } s && l > s)
        {
            failures.Add(
                $"{nameof(LatticeReplicationStatusOptions)}.{laggingName} must not exceed {stalledName}.");
        }
    }
}

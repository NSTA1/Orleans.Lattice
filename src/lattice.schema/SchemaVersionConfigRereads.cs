using System.Collections.Concurrent;
using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Schema;

/// <summary>
/// The bounded re-read of a tree's version config taken before a value stamped
/// above this silo's cached target is refused (#5711, #5712). A target advance
/// reaches every other silo only within
/// <see cref="LatticeSchemaVersioningOptions.PropagationInterval"/>, but the silo
/// that served it stamps writes at the new target at once, so such a value may be
/// current rather than newer. The re-read is single-flight per tree: concurrent
/// callers share one version-store read. After a re-read, no other re-read of
/// that tree starts for one propagation interval, so a value that really is newer
/// than the target costs at most one store read per tree per interval. Callers
/// take it only on that cold path; nothing else pays for it.
/// </summary>
internal sealed class SchemaVersionConfigRereads(
    ILatticeSchemaVersionProvider provider,
    IOptions<LatticeSchemaVersioningOptions>? options = null,
    TimeProvider? timeProvider = null)
{
    private readonly ConcurrentDictionary<string, ConfigReread> _rereads = new(StringComparer.Ordinal);
    private readonly TimeProvider _time = timeProvider ?? TimeProvider.System;
    private readonly TimeSpan _rereadWindow = options?.Value.PropagationInterval ?? new LatticeSchemaVersioningOptions().PropagationInterval;

    /// <summary>
    /// Re-reads <paramref name="treeId"/>'s config after a value was found stamped
    /// above <paramref name="cached"/>'s target. Joins a re-read already in flight;
    /// within the re-read window of the last one, returns its answer without a new
    /// read. A failed re-read answers <paramref name="cached"/>, so the value is
    /// judged exactly as it was before.
    /// </summary>
    public async Task<LatticeSchemaVersionConfig?> RereadAsync(string treeId, LatticeSchemaVersionConfig cached)
    {
        while (true)
        {
            var now = _time.GetTimestamp();
            if (_rereads.TryGetValue(treeId, out var last)
                && (!last.Read.IsCompleted || _time.GetElapsedTime(last.StartedAt, now) < _rereadWindow))
            {
                return await last.Read.ConfigureAwait(false);
            }

            var completion = new TaskCompletionSource<LatticeSchemaVersionConfig?>(TaskCreationOptions.RunContinuationsAsynchronously);
            var mine = new ConfigReread(completion.Task, now);
            if (last is null)
            {
                if (_rereads.Count >= SchemaTreeCacheExtensions.MaxCachedTrees)
                {
                    _rereads.Clear();
                }

                if (!_rereads.TryAdd(treeId, mine))
                {
                    continue;
                }
            }
            else if (!_rereads.TryUpdate(treeId, mine, last))
            {
                continue;
            }

            LatticeSchemaVersionConfig? config;
            try
            {
                provider.Invalidate(treeId);
                config = await provider.GetConfigAsync(treeId, CancellationToken.None).ConfigureAwait(false);
            }
            catch (Exception)
            {
                config = cached;
            }

            completion.SetResult(config);
            return config;
        }
    }

    /// <summary>One re-read of a tree's config: its shared result and when it started.</summary>
    private sealed class ConfigReread(Task<LatticeSchemaVersionConfig?> read, long startedAt)
    {
        public Task<LatticeSchemaVersionConfig?> Read { get; } = read;

        public long StartedAt { get; } = startedAt;
    }
}

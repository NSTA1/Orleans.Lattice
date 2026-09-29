using System.Collections.Concurrent;
using Orleans.Lattice.Api.Apps;

namespace Orleans.Lattice.Explorer.UiTests;

/// <summary>
/// Stands in for the cluster behind the app bridge: it answers nothing useful and
/// records every call, so a test can prove the broker refused a request before it
/// reached the cluster.
/// </summary>
internal sealed class RecordingBridge : ILatticeAppBridge
{
    private readonly ConcurrentQueue<string> _calls = new();

    /// <summary>Every call, as <c>operation tree key</c>, in order.</summary>
    public IReadOnlyCollection<string> Calls => _calls;

    /// <summary>Forgets every recorded call.</summary>
    public void Clear() => _calls.Clear();

    /// <inheritdoc />
    public Task<AppBridgeValue?> GetAsync(AppBridgeTarget target, string key, CancellationToken cancellationToken = default)
    {
        Record("get", target, key);
        return Task.FromResult<AppBridgeValue?>(null);
    }

    /// <inheritdoc />
    public Task<AppBridgePage> ScanAsync(AppBridgeTarget target, string prefix, int pageSize, string? continuation = null, CancellationToken cancellationToken = default)
    {
        Record("scan", target, prefix);
        return Task.FromResult(new AppBridgePage());
    }

    /// <inheritdoc />
    public Task SetAsync(AppBridgeTarget target, string key, ReadOnlyMemory<byte> value, CancellationToken cancellationToken = default)
    {
        Record("set", target, key);
        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public Task<bool> DeleteAsync(AppBridgeTarget target, string key, CancellationToken cancellationToken = default)
    {
        Record("delete", target, key);
        return Task.FromResult(false);
    }

    private void Record(string operation, AppBridgeTarget target, string key) =>
        _calls.Enqueue($"{operation} {target} {key}");
}

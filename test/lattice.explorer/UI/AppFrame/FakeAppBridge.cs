using Orleans.Lattice.Api.Apps;

namespace Orleans.Lattice.Explorer.Tests.UI.Framing;

/// <summary>
/// A scriptable <see cref="ILatticeAppBridge"/> that records every target it was called
/// with. Setting <see cref="Gate"/> holds every call open until the test completes it, which
/// is how the concurrency limit is exercised without any timing.
/// </summary>
internal sealed class FakeAppBridge : ILatticeAppBridge
{
    /// <summary>Every call, in order: the operation name, target and key.</summary>
    public List<(string Operation, AppBridgeTarget Target, string Key)> Calls { get; } = [];

    /// <summary>The values <see cref="GetAsync"/> returns, by key.</summary>
    public Dictionary<string, byte[]> Values { get; } = new(StringComparer.Ordinal);

    /// <summary>The page <see cref="ScanAsync"/> returns.</summary>
    public AppBridgePage Page { get; set; } = new();

    /// <summary>The prefix, page size and continuation of the last scan.</summary>
    public (string Prefix, int PageSize, string? Continuation) LastScan { get; private set; }

    /// <summary>The bytes of the last write.</summary>
    public byte[]? LastWrite { get; private set; }

    /// <summary>What <see cref="DeleteAsync"/> returns.</summary>
    public bool DeleteResult { get; set; } = true;

    /// <summary>When set, every call throws it.</summary>
    public Exception? Throw { get; set; }

    /// <summary>When set, every call waits for it before answering.</summary>
    public TaskCompletionSource? Gate { get; set; }

    /// <inheritdoc />
    public async Task<AppBridgeValue?> GetAsync(AppBridgeTarget target, string key, CancellationToken cancellationToken = default)
    {
        await EnterAsync("get", target, key);
        return Values.TryGetValue(key, out var value) ? new AppBridgeValue { Key = key, Value = value } : null;
    }

    /// <inheritdoc />
    public async Task<AppBridgePage> ScanAsync(
        AppBridgeTarget target,
        string prefix,
        int pageSize,
        string? continuation = null,
        CancellationToken cancellationToken = default)
    {
        await EnterAsync("scan", target, prefix);
        LastScan = (prefix, pageSize, continuation);
        return Page;
    }

    /// <inheritdoc />
    public async Task SetAsync(AppBridgeTarget target, string key, ReadOnlyMemory<byte> value, CancellationToken cancellationToken = default)
    {
        await EnterAsync("set", target, key);
        LastWrite = value.ToArray();
    }

    /// <inheritdoc />
    public async Task<bool> DeleteAsync(AppBridgeTarget target, string key, CancellationToken cancellationToken = default)
    {
        await EnterAsync("delete", target, key);
        return DeleteResult;
    }

    private async Task EnterAsync(string operation, AppBridgeTarget target, string key)
    {
        Calls.Add((operation, target, key));
        if (Gate is { } gate)
        {
            await gate.Task;
        }

        if (Throw is not null)
        {
            throw Throw;
        }
    }
}

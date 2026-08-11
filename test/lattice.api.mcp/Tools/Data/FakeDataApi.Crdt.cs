using Orleans.Lattice.Api.Data;

namespace Orleans.Lattice.Api.Mcp.Tests;

/// <summary>
/// Typed-CRDT half of the <see cref="FakeDataApi"/> test double: deterministic
/// in-memory models of each primitive, sufficient to prove the data tools and the
/// tool-core mapping drive the right facade verb and shape the result correctly.
/// Convergence fidelity is intentionally minimal - a denied key throws on write
/// and reads as the empty value, exactly as the gated facade behaves.
/// </summary>
internal sealed partial class FakeDataApi
{
    private static readonly ByteArrayComparer ByteComparer = new();

    private readonly Dictionary<(string, string), Dictionary<string, long>> _counters = new();
    private readonly Dictionary<(string, string), Dictionary<string, long>> _gcounters = new();
    private readonly Dictionary<(string, string), HashSet<byte[]>> _sets = new();
    private readonly Dictionary<(string, string), HashSet<byte[]>> _rwSets = new();
    private readonly Dictionary<(string, string), bool> _orFlags = new();
    private readonly Dictionary<(string, string), bool> _rwFlags = new();
    private readonly Dictionary<(string, string), Dictionary<string, int>> _vectors = new();
    private readonly Dictionary<(string, string), List<byte[]>> _registers = new();
    private readonly Dictionary<(string, string), byte[]> _maxRegisters = new();
    private readonly Dictionary<(string, string), byte[]> _minRegisters = new();
    private readonly Dictionary<(string, string), List<byte[]>> _sequences = new();
    private readonly Dictionary<(string, string), Dictionary<string, List<byte[]>>> _maps = new();

    public Task CounterIncrementAsync(string treeId, string key, string replicaId, long amount, CancellationToken cancellationToken = default)
    {
        ThrowIfDenied(treeId, key);
        var replicas = _counters.TryGetValue((treeId, key), out var r) ? r : _counters[(treeId, key)] = new();
        replicas[replicaId] = replicas.GetValueOrDefault(replicaId) + amount;
        return Task.CompletedTask;
    }

    public Task CounterDecrementAsync(string treeId, string key, string replicaId, long amount, CancellationToken cancellationToken = default)
        => CounterIncrementAsync(treeId, key, replicaId, -amount, cancellationToken);

    public Task<long> CounterGetAsync(string treeId, string key, CancellationToken cancellationToken = default)
    {
        if (Denied.Contains((treeId, key)) || !_counters.TryGetValue((treeId, key), out var replicas))
        {
            return Task.FromResult(0L);
        }

        return Task.FromResult(replicas.Values.Sum());
    }

    public Task GCounterIncrementAsync(string treeId, string key, string replicaId, long amount, CancellationToken cancellationToken = default)
    {
        ThrowIfDenied(treeId, key);
        var replicas = _gcounters.TryGetValue((treeId, key), out var r) ? r : _gcounters[(treeId, key)] = new();
        replicas[replicaId] = replicas.GetValueOrDefault(replicaId) + amount;
        return Task.CompletedTask;
    }

    public Task<long> GCounterGetAsync(string treeId, string key, CancellationToken cancellationToken = default)
    {
        if (Denied.Contains((treeId, key)) || !_gcounters.TryGetValue((treeId, key), out var replicas))
        {
            return Task.FromResult(0L);
        }

        return Task.FromResult(replicas.Values.Sum());
    }

    public Task SetAddAsync(string treeId, string key, byte[] element, string replicaId, CancellationToken cancellationToken = default)
    {
        ThrowIfDenied(treeId, key);
        var set = _sets.TryGetValue((treeId, key), out var s) ? s : _sets[(treeId, key)] = new(ByteComparer);
        set.Add(element);
        return Task.CompletedTask;
    }

    public Task SetRemoveAsync(string treeId, string key, byte[] element, CancellationToken cancellationToken = default)
    {
        ThrowIfDenied(treeId, key);
        if (_sets.TryGetValue((treeId, key), out var set))
        {
            set.Remove(element);
        }

        return Task.CompletedTask;
    }

    public Task<IReadOnlyList<byte[]>> SetGetAsync(string treeId, string key, CancellationToken cancellationToken = default)
    {
        if (Denied.Contains((treeId, key)) || !_sets.TryGetValue((treeId, key), out var set))
        {
            return Task.FromResult<IReadOnlyList<byte[]>>([]);
        }

        return Task.FromResult<IReadOnlyList<byte[]>>([.. set]);
    }

    public Task OrFlagEnableAsync(string treeId, string key, string replicaId, CancellationToken cancellationToken = default)
    {
        ThrowIfDenied(treeId, key);
        _orFlags[(treeId, key)] = true;
        return Task.CompletedTask;
    }

    public Task OrFlagDisableAsync(string treeId, string key, CancellationToken cancellationToken = default)
    {
        ThrowIfDenied(treeId, key);
        _orFlags[(treeId, key)] = false;
        return Task.CompletedTask;
    }

    public Task<bool> OrFlagGetAsync(string treeId, string key, CancellationToken cancellationToken = default)
        => Task.FromResult(!Denied.Contains((treeId, key)) && _orFlags.GetValueOrDefault((treeId, key)));

    public Task RwFlagEnableAsync(string treeId, string key, string replicaId, CancellationToken cancellationToken = default)
    {
        ThrowIfDenied(treeId, key);
        _rwFlags[(treeId, key)] = true;
        return Task.CompletedTask;
    }

    public Task RwFlagDisableAsync(string treeId, string key, string replicaId, CancellationToken cancellationToken = default)
    {
        ThrowIfDenied(treeId, key);
        _rwFlags[(treeId, key)] = false;
        return Task.CompletedTask;
    }

    public Task<bool> RwFlagGetAsync(string treeId, string key, CancellationToken cancellationToken = default)
        => Task.FromResult(!Denied.Contains((treeId, key)) && _rwFlags.GetValueOrDefault((treeId, key)));

    public Task RwSetAddAsync(string treeId, string key, byte[] element, string replicaId, CancellationToken cancellationToken = default)
    {
        ThrowIfDenied(treeId, key);
        var set = _rwSets.TryGetValue((treeId, key), out var s) ? s : _rwSets[(treeId, key)] = new(ByteComparer);
        set.Add(element);
        return Task.CompletedTask;
    }

    public Task RwSetRemoveAsync(string treeId, string key, byte[] element, string replicaId, CancellationToken cancellationToken = default)
    {
        ThrowIfDenied(treeId, key);
        if (_rwSets.TryGetValue((treeId, key), out var set))
        {
            set.Remove(element);
        }

        return Task.CompletedTask;
    }

    public Task<IReadOnlyList<byte[]>> RwSetGetAsync(string treeId, string key, CancellationToken cancellationToken = default)
    {
        if (Denied.Contains((treeId, key)) || !_rwSets.TryGetValue((treeId, key), out var set))
        {
            return Task.FromResult<IReadOnlyList<byte[]>>([]);
        }

        return Task.FromResult<IReadOnlyList<byte[]>>([.. set]);
    }

    public Task VersionVectorTickAsync(string treeId, string key, string replicaId, CancellationToken cancellationToken = default)
    {
        ThrowIfDenied(treeId, key);
        var vector = _vectors.TryGetValue((treeId, key), out var v) ? v : _vectors[(treeId, key)] = new();
        vector[replicaId] = vector.GetValueOrDefault(replicaId) + 1;
        return Task.CompletedTask;
    }

    public Task<IReadOnlyDictionary<string, string>> VersionVectorGetAsync(string treeId, string key, CancellationToken cancellationToken = default)
    {
        if (Denied.Contains((treeId, key)) || !_vectors.TryGetValue((treeId, key), out var vector))
        {
            return Task.FromResult<IReadOnlyDictionary<string, string>>(new Dictionary<string, string>());
        }

        var result = new Dictionary<string, string>(vector.Count);
        foreach (var (replicaId, counter) in vector)
        {
            result[replicaId] = $"{counter}:0";
        }

        return Task.FromResult<IReadOnlyDictionary<string, string>>(result);
    }

    public Task RegisterSetAsync(string treeId, string key, string replicaId, byte[] value, CancellationToken cancellationToken = default)
    {
        ThrowIfDenied(treeId, key);
        _registers[(treeId, key)] = [value];
        return Task.CompletedTask;
    }

    public Task<IReadOnlyList<byte[]>> RegisterGetAsync(string treeId, string key, CancellationToken cancellationToken = default)
    {
        if (Denied.Contains((treeId, key)) || !_registers.TryGetValue((treeId, key), out var values))
        {
            return Task.FromResult<IReadOnlyList<byte[]>>([]);
        }

        return Task.FromResult<IReadOnlyList<byte[]>>([.. values]);
    }

    public Task MaxRegisterSetAsync(string treeId, string key, byte[] value, CancellationToken cancellationToken = default)
    {
        ThrowIfDenied(treeId, key);
        if (!_maxRegisters.TryGetValue((treeId, key), out var current)
            || ((ReadOnlySpan<byte>)value).SequenceCompareTo(current) > 0)
        {
            _maxRegisters[(treeId, key)] = value;
        }

        return Task.CompletedTask;
    }

    public Task<byte[]?> MaxRegisterGetAsync(string treeId, string key, CancellationToken cancellationToken = default)
    {
        if (Denied.Contains((treeId, key)) || !_maxRegisters.TryGetValue((treeId, key), out var value))
        {
            return Task.FromResult<byte[]?>(null);
        }

        return Task.FromResult<byte[]?>(value);
    }

    public Task MinRegisterSetAsync(string treeId, string key, byte[] value, CancellationToken cancellationToken = default)
    {
        ThrowIfDenied(treeId, key);
        if (!_minRegisters.TryGetValue((treeId, key), out var current)
            || ((ReadOnlySpan<byte>)value).SequenceCompareTo(current) < 0)
        {
            _minRegisters[(treeId, key)] = value;
        }

        return Task.CompletedTask;
    }

    public Task<byte[]?> MinRegisterGetAsync(string treeId, string key, CancellationToken cancellationToken = default)
    {
        if (Denied.Contains((treeId, key)) || !_minRegisters.TryGetValue((treeId, key), out var value))
        {
            return Task.FromResult<byte[]?>(null);
        }

        return Task.FromResult<byte[]?>(value);
    }

    public Task SequenceInsertAtAsync(string treeId, string key, int index, string replicaId, byte[] value, CancellationToken cancellationToken = default)
    {
        ThrowIfDenied(treeId, key);
        var list = _sequences.TryGetValue((treeId, key), out var s) ? s : _sequences[(treeId, key)] = new();
        list.Insert(Math.Clamp(index, 0, list.Count), value);
        return Task.CompletedTask;
    }

    public Task SequenceRemoveAtAsync(string treeId, string key, int index, CancellationToken cancellationToken = default)
    {
        ThrowIfDenied(treeId, key);
        if (_sequences.TryGetValue((treeId, key), out var list) && index >= 0 && index < list.Count)
        {
            list.RemoveAt(index);
        }

        return Task.CompletedTask;
    }

    public Task<IReadOnlyList<byte[]>> SequenceGetAsync(string treeId, string key, CancellationToken cancellationToken = default)
    {
        if (Denied.Contains((treeId, key)) || !_sequences.TryGetValue((treeId, key), out var list))
        {
            return Task.FromResult<IReadOnlyList<byte[]>>([]);
        }

        return Task.FromResult<IReadOnlyList<byte[]>>([.. list]);
    }

    public Task MapSetAsync(string treeId, string key, string field, string replicaId, byte[] value, CancellationToken cancellationToken = default)
    {
        ThrowIfDenied(treeId, key);
        var map = _maps.TryGetValue((treeId, key), out var m) ? m : _maps[(treeId, key)] = new();
        map[field] = [value];
        return Task.CompletedTask;
    }

    public Task MapRemoveAsync(string treeId, string key, string field, CancellationToken cancellationToken = default)
    {
        ThrowIfDenied(treeId, key);
        if (_maps.TryGetValue((treeId, key), out var map))
        {
            map.Remove(field);
        }

        return Task.CompletedTask;
    }

    public Task<IReadOnlyDictionary<string, IReadOnlyList<byte[]>>> MapGetAsync(string treeId, string key, CancellationToken cancellationToken = default)
    {
        if (Denied.Contains((treeId, key)) || !_maps.TryGetValue((treeId, key), out var map))
        {
            return Task.FromResult<IReadOnlyDictionary<string, IReadOnlyList<byte[]>>>(
                new Dictionary<string, IReadOnlyList<byte[]>>());
        }

        var result = new Dictionary<string, IReadOnlyList<byte[]>>(map.Count);
        foreach (var (field, values) in map)
        {
            result[field] = [.. values];
        }

        return Task.FromResult<IReadOnlyDictionary<string, IReadOnlyList<byte[]>>>(result);
    }

    private readonly Dictionary<(string, string), HashSet<byte[]>> _gsets = new();

    public Task GSetAddAsync(string treeId, string key, byte[] element, CancellationToken cancellationToken = default)
    {
        ThrowIfDenied(treeId, key);
        var set = _gsets.TryGetValue((treeId, key), out var s) ? s : _gsets[(treeId, key)] = new(ByteComparer);
        set.Add(element);
        return Task.CompletedTask;
    }

    public Task<IReadOnlyList<byte[]>> GSetGetAsync(string treeId, string key, CancellationToken cancellationToken = default)
    {
        if (Denied.Contains((treeId, key)) || !_gsets.TryGetValue((treeId, key), out var set))
        {
            return Task.FromResult<IReadOnlyList<byte[]>>([]);
        }

        return Task.FromResult<IReadOnlyList<byte[]>>([.. set]);
    }

    private sealed class ByteArrayComparer : IEqualityComparer<byte[]>
    {
        public bool Equals(byte[]? x, byte[]? y) => x is not null && y is not null && x.AsSpan().SequenceEqual(y);

        public int GetHashCode(byte[] obj)
        {
            var hash = new HashCode();
            hash.AddBytes(obj);
            return hash.ToHashCode();
        }
    }
}

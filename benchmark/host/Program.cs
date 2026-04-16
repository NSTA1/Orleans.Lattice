using System.Threading;
using Orleans.Lattice;
using Orleans.Lattice.BPlusTree;

var builder = WebApplication.CreateBuilder(args);

builder.UseOrleans(silo =>
{
    silo.UseLocalhostClustering();
    silo.UseInMemoryReminderService();
    silo.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
});

var app = builder.Build();

var api = app.MapGroup("/lattice/{treeId}");

// ── Original CRUD endpoints ─────────────────────────────────────────────

// GET /lattice/{treeId}/keys/{key}
api.MapGet("/keys/{key}", async (string treeId, string key, IGrainFactory grains) =>
{
    var grain = grains.GetGrain<ILattice>(treeId);
    var value = await grain.GetAsync(key);
    return value is null ? Results.NotFound() : Results.Bytes(value, "application/octet-stream");
});

// PUT /lattice/{treeId}/keys/{key}
api.MapPut("/keys/{key}", async (string treeId, string key, HttpRequest request, IGrainFactory grains) =>
{
    using var ms = new MemoryStream();
    await request.Body.CopyToAsync(ms);
    var grain = grains.GetGrain<ILattice>(treeId);
    await grain.SetAsync(key, ms.ToArray());
    return Results.NoContent();
});

// DELETE /lattice/{treeId}/keys/{key}
api.MapDelete("/keys/{key}", async (string treeId, string key, IGrainFactory grains) =>
{
    var grain = grains.GetGrain<ILattice>(treeId);
    var deleted = await grain.DeleteAsync(key);
    return deleted ? Results.NoContent() : Results.NotFound();
});

// GET /lattice/{treeId}/keys?start=&end=&reverse=false
api.MapGet("/keys", (string treeId, string? start, string? end, bool? reverse, IGrainFactory grains) =>
{
    var grain = grains.GetGrain<ILattice>(treeId);
    var keys = grain.KeysAsync(start, end, reverse ?? false);

    async IAsyncEnumerable<string> Stream()
    {
        await foreach (var k in keys)
        {
            yield return k;
        }
    }

    return Stream();
});

// ── Benchmark helpers ───────────────────────────────────────────────────
// These endpoints use atomic counters so bombardier (which sends the same
// URL every time) exercises a different key on each request.
//
// Expected flow:
//   1. POST /bench/configure   — set key approach (ordered|random|reverse) and key count
//   2. POST /bench/set         — writes keys according to the configured approach
//   3. POST /bench/reset       — snapshots the SET high-water mark for GET/DELETE
//   4. GET  /bench/get         — reads keys cyclically from the written list
//   5. DELETE /bench/delete    — deletes keys cyclically from the written list
//   6. POST /seed?count=N      — small seed for KEYS on a separate tree

var setCounter    = new Counter();
var getCounter    = new Counter();
var deleteCounter = new Counter();
var keyStrategy   = new KeyStrategy();

// POST /lattice/{treeId}/bench/configure?approach=ordered|random|reverse&keyCount=N
// Configures the key generation approach and resets all counters.
api.MapPost("/bench/configure", (string approach, int keyCount) =>
{
    keyStrategy.Configure(approach, keyCount);
    setCounter.Reset();
    getCounter.Reset();
    deleteCounter.Reset();
    return Results.Ok(new { approach = keyStrategy.Approach, keyCount });
});

// POST /lattice/{treeId}/seed?count=N — bulk-insert keys k0..k{N-1}.
api.MapPost("/seed", async (string treeId, int count, IGrainFactory grains) =>
{
    var grain = grains.GetGrain<ILattice>(treeId);
    var value = "benchmarkvalue"u8.ToArray();
    for (var i = 0; i < count; i++)
        await grain.SetAsync($"k{i}", value);

    return Results.Ok(new { seeded = count });
});

// POST /lattice/{treeId}/bench/reset — reset GET/DELETE counters to cycle
// over the keys that were actually written during the SET phase.
api.MapPost("/bench/reset", () =>
{
    var kc = keyStrategy.WrittenCount;
    getCounter.Reset(kc);
    deleteCounter.Reset(kc);
    return Results.Ok(new { keyCount = kc });
});

// POST /lattice/{treeId}/bench/set — sets a key according to the configured approach
api.MapPost("/bench/set", async (string treeId, IGrainFactory grains) =>
{
    var i = setCounter.Next();
    var key = keyStrategy.GetKeyForWrite(i);
    var grain = grains.GetGrain<ILattice>(treeId);
    await grain.SetAsync(key, "benchmarkvalue"u8.ToArray());
    return Results.NoContent();
});

// GET /lattice/{treeId}/bench/get — reads keys cyclically from the written list
api.MapGet("/bench/get", async (string treeId, IGrainFactory grains) =>
{
    var i = getCounter.NextCyclic();
    var key = keyStrategy.GetWrittenKey(i);
    var grain = grains.GetGrain<ILattice>(treeId);
    var value = await grain.GetAsync(key);
    return value is null ? Results.NotFound() : Results.Bytes(value, "application/octet-stream");
});

// DELETE /lattice/{treeId}/bench/delete — deletes keys cyclically from the written list.
// Returns 204 even for already-deleted keys (idempotent) so that cyclic
// wrap-around doesn't produce misleading 404s in the benchmark summary.
// Real 4xx/5xx errors (bad routes, server failures) still surface.
api.MapDelete("/bench/delete", async (string treeId, IGrainFactory grains) =>
{
    var i = deleteCounter.NextCyclic();
    var key = keyStrategy.GetWrittenKey(i);
    var grain = grains.GetGrain<ILattice>(treeId);
    await grain.DeleteAsync(key);
    return Results.NoContent();
});

// POST /lattice/{treeId}/bench/bulkload — bulk-loads keys using BulkLoadAsync.
// Uses a dedicated tree per request so each load starts from empty.
// Fixed batch of 1000 keys per request keeps per-request cost comparable
// to SET (which inserts 1 key) while still exercising the bulk-load path.
// The meaningful comparison is keys/second: BULK-SET RPS × 1000 vs SET RPS × 1.
const int BulkLoadBatchSize = 1000;
var bulkLoadCounter = new Counter();

api.MapPost("/bench/bulkload", async (string treeId, IGrainFactory grains) =>
{
    var batch = bulkLoadCounter.Next();
    var bulkTreeId = $"{treeId}-bulk-{batch}";
    var grain = grains.GetGrain<ILattice>(bulkTreeId);
    var value = "benchmarkvalue"u8.ToArray();

    var entries = new List<KeyValuePair<string, byte[]>>(BulkLoadBatchSize);
    for (int i = 0; i < BulkLoadBatchSize; i++)
        entries.Add(KeyValuePair.Create($"k{i:D6}", value));

    await grain.BulkLoadAsync(entries);
    return Results.NoContent();
});

app.Run();

/// <summary>
/// A thread-safe counter that supports unbounded increment and cyclic wrap-around.
/// </summary>
sealed class Counter
{
    private int _value;
    private int _mod = 1;

    /// <summary>The current (non-incremented) counter value — i.e. the high-water mark.</summary>
    public int Current => Volatile.Read(ref _value);

    public void Reset(int mod = 1)
    {
        _mod = mod < 1 ? 1 : mod;
        Interlocked.Exchange(ref _value, 0);
    }

    /// <summary>Returns the next value (unbounded).</summary>
    public int Next() => Interlocked.Increment(ref _value) - 1;

    /// <summary>Returns the next value (zero-based), wrapping around at the configured modulus.</summary>
    public int NextCyclic() => (Interlocked.Increment(ref _value) - 1) % _mod;
}

/// <summary>
/// Generates keys according to one of three approaches and tracks every key
/// written during the SET phase. GET/DELETE read directly from the written
/// list so there is no mapping mismatch.
/// <list type="bullet">
/// <item><c>ordered</c> — <c>k000000</c>, <c>k000001</c>, … (ascending)</item>
/// <item><c>reverse</c> — <c>k{N-1}</c>, <c>k{N-2}</c>, … (descending)</item>
/// <item><c>random</c>  — pre-shuffled permutation of <c>k000000</c>…<c>k{N-1}</c></item>
/// </list>
/// </summary>
sealed class KeyStrategy
{
    private string _approach = "ordered";
    private int _keyCount = 100_000;
    private string[]? _shuffled;
    private readonly List<string> _written = [];
    private readonly Lock _writeLock = new();

    public string Approach => _approach;
    public int WrittenCount => _written.Count;

    public void Configure(string approach, int keyCount)
    {
        _approach = approach.ToLowerInvariant();
        _keyCount = keyCount < 1 ? 100_000 : keyCount;
        _shuffled = null;

        lock (_writeLock)
            _written.Clear();

        if (_approach == "random")
        {
            _shuffled = new string[_keyCount];
            for (int i = 0; i < _keyCount; i++)
                _shuffled[i] = $"k{i:D6}";
            var rng = Random.Shared;
            for (int i = _keyCount - 1; i > 0; i--)
            {
                int j = rng.Next(i + 1);
                (_shuffled[i], _shuffled[j]) = (_shuffled[j], _shuffled[i]);
            }
        }
    }

    /// <summary>
    /// Generates the key for the given SET index and records it in the written list.
    /// </summary>
    public string GetKeyForWrite(int index)
    {
        var key = _approach switch
        {
            "reverse" => $"k{(_keyCount - 1 - (index % _keyCount)):D6}",
            "random"  => _shuffled![index % _shuffled.Length],
            _         => $"k{index:D6}",
        };

        lock (_writeLock)
            _written.Add(key);

        return key;
    }

    /// <summary>
    /// Returns a key from the written list by index (wrapping if needed).
    /// </summary>
    public string GetWrittenKey(int index)
    {
        lock (_writeLock)
            return _written[index % _written.Count];
    }
}

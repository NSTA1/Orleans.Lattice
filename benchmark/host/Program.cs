using System.Threading;
using Orleans.Lattice;
using Orleans.Lattice.BPlusTree;

var builder = WebApplication.CreateBuilder(args);

builder.UseOrleans(silo =>
{
    silo.UseLocalhostClustering();
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
//   1. POST /bench/set        — writes k0, k1, k2, … (unbounded counter)
//   2. POST /bench/reset      — snapshots the SET high-water mark for GET/DELETE
//   3. GET  /bench/get        — reads k0 … k{hwm-1} cyclically
//   4. DELETE /bench/delete   — deletes k0, k1, … (unbounded, each key once)
//   5. POST /seed?count=N     — small seed for KEYS on a separate tree

var setCounter    = new Counter();
var getCounter    = new Counter();
var deleteCounter = new Counter();

// POST /lattice/{treeId}/seed?count=N — bulk-insert keys k0..k{N-1}.
api.MapPost("/seed", async (string treeId, int count, IGrainFactory grains) =>
{
    var grain = grains.GetGrain<ILattice>(treeId);
    var value = "benchmarkvalue"u8.ToArray();
    for (var i = 0; i < count; i++)
        await grain.SetAsync($"k{i}", value);

    return Results.Ok(new { seeded = count });
});

// POST /lattice/{treeId}/bench/reset?keyCount=N — reset GET/DELETE counters.
// keyCount tells GET how many keys exist so it can cycle; DELETE resets to 0.
api.MapPost("/bench/reset", (int? keyCount) =>
{
    var kc = keyCount ?? setCounter.Current;
    getCounter.Reset(kc);
    deleteCounter.Reset();
    return Results.Ok(new { keyCount = kc });
});

// POST /lattice/{treeId}/bench/set — sets key k{N} with each request
api.MapPost("/bench/set", async (string treeId, IGrainFactory grains) =>
{
    var i = setCounter.Next();
    var grain = grains.GetGrain<ILattice>(treeId);
    await grain.SetAsync($"k{i}", "benchmarkvalue"u8.ToArray());
    return Results.NoContent();
});

// GET /lattice/{treeId}/bench/get — reads key k{N % keyCount}, cycling through written keys
api.MapGet("/bench/get", async (string treeId, IGrainFactory grains) =>
{
    var i = getCounter.NextCyclic();
    var grain = grains.GetGrain<ILattice>(treeId);
    var value = await grain.GetAsync($"k{i}");
    return value is null ? Results.NotFound() : Results.Bytes(value, "application/octet-stream");
});

// DELETE /lattice/{treeId}/bench/delete — deletes key k{N}, each key at most once
api.MapDelete("/bench/delete", async (string treeId, IGrainFactory grains) =>
{
    var i = deleteCounter.Next();
    var grain = grains.GetGrain<ILattice>(treeId);
    var deleted = await grain.DeleteAsync($"k{i}");
    return deleted ? Results.NoContent() : Results.NotFound();
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

    /// <summary>Returns the next value, wrapping around at the configured modulus.</summary>
    public int NextCyclic() => Interlocked.Increment(ref _value) % _mod;
}

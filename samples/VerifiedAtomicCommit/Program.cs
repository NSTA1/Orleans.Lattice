using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Orleans.Hosting;
using Orleans.Lattice;

// ---------------------------------------------------------------------------
// VerifiedAtomicCommit - the observable all-or-nothing property that the
// atomic-commit protocol cores, the Coyote concurrency tier, and the TLA+ spec
// machine-check.
//
// The other atomic-write sample narrates a single saga step-by-step. This one
// makes the *verified property* observable at runtime: a concurrent snapshot
// reader races a saga that repeatedly flips a set of keys between a "pre" and a
// "post" value, and we prove the reader NEVER observes a torn view - every
// snapshot is all-pre or all-post, never a mix. That AllOrNothing /
// VisibilityMatchesDecision property is exactly what:
//   * the production core AtomicVisibilityGate.ResolveKey decides on the hot
//     path (src/lattice/BPlusTree/),
//   * the Coyote models assert under every interleaving
//     (test/lattice/BPlusTree/Coyote/), and
//   * the TLA+ spec checks over bounded instances (spec/AtomicCommit.tla).
// See docs/lattice/verified-atomic-commit.md.
// ---------------------------------------------------------------------------

using var host = Host.CreateDefaultBuilder(args)
    .ConfigureLogging(logging =>
    {
        // Silence Orleans so the console shows only the feature narration.
        logging.ClearProviders();
        logging.SetMinimumLevel(LogLevel.None);
    })
    .UseOrleans(silo =>
    {
        silo.UseLocalhostClustering();
        silo.AddMemoryGrainStorageAsDefault();
        silo.UseInMemoryReminderService();
        silo.AddLattice((s, name) => s.AddMemoryGrainStorage(name));
    })
    .Build();

await host.StartAsync();
var grainFactory = host.Services.GetRequiredService<IGrainFactory>();

Console.WriteLine("== VerifiedAtomicCommit sample ==");
Console.WriteLine();

var ledger = grainFactory.GetGrain<ILattice>("ledger");

// The saga's key set. A single atomic batch flips ALL of them together, so a
// snapshot read must observe them all at one value or all at the other.
var keys = new List<string> { "acct:a/balance", "acct:b/balance", "acct:c/balance", "journal:last" };

static List<KeyValuePair<string, byte[]>> Batch(List<string> keys, string tag) =>
    keys.Select(k => new KeyValuePair<string, byte[]>(k, Encoding.UTF8.GetBytes($"{k}={tag}"))).ToList();

var preBatch = Batch(keys, "PRE");
var postBatch = Batch(keys, "POST");

// Seed the pre-state as one atomic batch.
await ledger.SetManyAtomicAsync(preBatch);
Console.WriteLine($"Seeded {keys.Count} keys at their PRE value as one atomic batch.");
Console.WriteLine();

// --- 1. Concurrent snapshot reader races the saga ---------------------------
// GetManyAsync is the public N-key snapshot read: it resolves every key against
// one registry-revision snapshot (the reader-stability path), so it observes a
// saga all-or-nothing. We hammer it from a reader task while a writer task flips
// the whole key set back and forth many times, and classify every snapshot.
Console.WriteLine("1) Racing a concurrent snapshot reader against a flipping saga...");

const int rounds = 200;
using var done = new CancellationTokenSource();

long snapshots = 0, allPre = 0, allPost = 0, torn = 0;

var reader = Task.Run(async () =>
{
    while (!done.IsCancellationRequested)
    {
        var snap = await ledger.GetManyAsync(keys);
        if (snap.Count != keys.Count)
        {
            continue; // seeding not yet complete for every key
        }

        var tags = snap.Values
            .Select(v => Encoding.UTF8.GetString(v))
            .Select(s => s.EndsWith("=POST", StringComparison.Ordinal) ? "POST" : "PRE")
            .ToHashSet();

        snapshots++;
        if (tags.Count > 1)
        {
            torn++; // a single snapshot saw both PRE and POST keys: a torn read
        }
        else if (tags.Contains("POST"))
        {
            allPost++;
        }
        else
        {
            allPre++;
        }
    }
});

for (var i = 0; i < rounds; i++)
{
    await ledger.SetManyAtomicAsync(i % 2 == 0 ? postBatch : preBatch);
}

done.Cancel();
await reader;

Console.WriteLine($"   Saga flips committed : {rounds}");
Console.WriteLine($"   Snapshots observed   : {snapshots}");
Console.WriteLine($"   ... all-PRE          : {allPre}");
Console.WriteLine($"   ... all-POST         : {allPost}");
Console.WriteLine($"   ... TORN (mixed)     : {torn}");
Console.WriteLine(torn == 0
    ? "   -> zero torn reads: every snapshot resolved the whole saga against one decision."
    : "   -> UNEXPECTED torn read detected!");
Console.WriteLine();

// --- 2. A failed guard leaves no partial state ------------------------------
// The all-or-nothing property also covers the abort path: a guarded batch whose
// precondition fails writes nothing, so every key keeps its pre-saga value. We
// seed two typed accounts, reject a batch whose guard one key fails, and confirm
// both keep their original balances (StrictIsolation on the abort path).
var bank = grainFactory.GetGrain<ILattice>("bank");
await bank.SetAsync("acct:a", new Account("acct:a", 120m));
await bank.SetAsync("acct:b", new Account("acct:b", 80m));
Console.WriteLine("2) Seeded acct:a=120, acct:b=80; rejecting a guarded transfer...");

var transfer = new List<KeyValuePair<string, Account>>
{
    new("acct:a", new Account("acct:a", 999m)),
    new("acct:b", new Account("acct:b", 5m)),
};

// Guard requires every current balance >= 100. acct:b is 80, so the whole batch
// is rejected and neither key changes.
var outcome = await bank.SetManyAtomicAsync<Account>(transfer, current => current.Balance >= 100m);

var a = await bank.GetAsync<Account>("acct:a");
var b = await bank.GetAsync<Account>("acct:b");
Console.WriteLine($"   Guard 'Balance >= 100' outcome: {outcome}");
Console.WriteLine($"   acct:a = {a!.Balance}, acct:b = {b!.Balance}");
Console.WriteLine(a.Balance == 120m && b.Balance == 80m
    ? "   -> both accounts keep their ORIGINAL balances: no partial write leaked."
    : "   -> UNEXPECTED partial state after a rejected batch!");
Console.WriteLine();

Console.WriteLine("This all-or-nothing visibility is machine-checked, not just observed here:");
Console.WriteLine("  * cores    : src/lattice/BPlusTree/ (AtomicVisibilityGate, SagaCoordinatorCore, ...)");
Console.WriteLine("  * Coyote   : test/lattice/BPlusTree/Coyote/  (dotnet test --filter Category=Coyote)");
Console.WriteLine("  * TLA+     : spec/AtomicCommit.tla");
Console.WriteLine("  * docs     : docs/lattice/verified-atomic-commit.md");
Console.WriteLine();
Console.WriteLine("Done.");

await host.StopAsync();

// A typed value for the guarded-write demonstration. Records serialize to JSON
// through the typed lattice extensions, so the server-side guard can evaluate a
// predicate over the current stored document.
public sealed record Account(string Id, decimal Balance);

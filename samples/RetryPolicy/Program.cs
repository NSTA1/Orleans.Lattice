using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Orleans.Hosting;
using Orleans.Lattice;

// ---------------------------------------------------------------------------
// RetryPolicy
// ---------------------------------------------------------------------------
// Every mutating Lattice method has throw-and-revert semantics by default: on a
// transient storage fault it throws, and the library never silently retries.
// Retry is opt-in and belongs to the CALLER'S environment.
//
// The opt-in surface is two cooperating pieces:
//   * LatticeIdempotencyContext - an ambient scope that pins one logical
//     mutation identity across attempts, so a retried write collapses to a
//     no-op instead of applying twice.
//   * ILatticeRetryPolicy (shipped default: BoundedExponentialRetryPolicy) -
//     re-invokes the operation under that same ambient key on a transient fault.
//
// This sample simulates a flaky storage layer: the first two attempts of an
// operation fail with a transient fault, the third succeeds. We run it inside
// an idempotency scope so all three attempts share one identity.
//
// See docs/lattice/retry-policy.md.
// ---------------------------------------------------------------------------

using var host = Host.CreateDefaultBuilder(args)
    .ConfigureLogging(logging =>
    {
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

Console.Write("Silo starting...");
await host.StartAsync();
Console.WriteLine(" ready.");
Console.WriteLine();

var grainFactory = host.Services.GetRequiredService<IGrainFactory>();
var tree = grainFactory.GetGrain<ILattice>("retry-demo");

// Build the shipped policy locally (the caller-side pattern from the docs).
// 3 attempts, short back-off, and a classifier that only retries the transient
// fault family - a real caller would scope this to their provider's transient
// exceptions so programmer errors are never retried.
var policy = new BoundedExponentialRetryPolicy(
    maxAttempts: 3,
    initialDelay: TimeSpan.FromMilliseconds(50),
    maxDelay: TimeSpan.FromMilliseconds(500),
    retryableExceptionClassifier: static ex => ex is TransientStorageException);

// A simulated flaky dependency: fails the first two calls, then succeeds.
var attempts = 0;

Console.WriteLine("== Retrying a write that fails twice under a simulated transient fault ==");

// LatticeIdempotencyContext.NewScope() mints one fresh key for the whole
// scope. Every retry re-stamps the same HLC on the leaf, so even if a "failed"
// attempt had partially landed, re-applying under the same identity is a no-op
// rather than a second mutation.
using (LatticeIdempotencyContext.NewScope())
{
    await policy.ExecuteAsync(async ct =>
    {
        attempts++;
        Console.WriteLine($"  attempt #{attempts}...");

        // Simulate a storage layer that is transiently unavailable.
        if (attempts < 3)
        {
            throw new TransientStorageException($"storage unavailable (attempt {attempts})");
        }

        // The actual mutation. It only runs once storage "recovers", but it is
        // executed under the same ambient idempotency key on every attempt.
        await tree.SetAsync("orders/42", Encoding.UTF8.GetBytes("shipped"), ct);
        Console.WriteLine("  attempt succeeded - write committed.");
    }, CancellationToken.None);
}

Console.WriteLine();

// Confirm exactly one logical value landed.
var stored = await tree.GetAsync("orders/42");
Console.WriteLine($"  tree['orders/42'] = {Encoding.UTF8.GetString(stored!)}  (after {attempts} attempts)");
Console.WriteLine();
Console.WriteLine("Done. The operation survived transient faults and the retried write");
Console.WriteLine("collapsed to a single mutation under one idempotency key.");

await host.StopAsync();

// A stand-in for the transient storage faults a real provider would raise
// (timeouts, socket errors, throttling). The retry classifier above matches
// exactly this type so nothing else is ever retried.
internal sealed class TransientStorageException(string message) : Exception(message);

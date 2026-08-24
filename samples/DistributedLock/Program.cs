using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Orleans.Hosting;
using Orleans.Lattice;

// ---------------------------------------------------------------------------
// DistributedLock - a cluster-wide, FIFO-fair distributed lock / lease.
//
// ILatticeLockGrain packages the single-threaded-grain FIFO mutual-exclusion
// pattern and gets the failure modes right: monotonic fencing tokens (so a
// superseded holder is detectable) and bounded leases (so a crashed holder cannot
// wedge the lock forever). This sample demonstrates:
//   1. Acquire -> fencing token -> renew -> release.
//   2. Non-blocking TryAcquire: null while held, granted after release.
//   3. FIFO blocking: a queued waiter is granted the instant the holder releases,
//      with a strictly higher fencing token.
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

// All callers naming the same lock contend for the same activation, serialized FIFO.
var theLock = grainFactory.GetGrain<ILatticeLockGrain>("inventory/sku-42");

Console.WriteLine("== DistributedLock sample ==");
Console.WriteLine();

// --- 1. Acquire, renew, release --------------------------------------------
Console.WriteLine("1) Acquiring the lock (30s lease, willing to wait 5s)...");
var lease = await theLock.AcquireAsync(new LockAcquireRequest(
    LeaseDuration: TimeSpan.FromSeconds(30),
    MaxWait: TimeSpan.FromSeconds(5)));
Console.WriteLine($"   granted, fencing token = {lease.Token.FencingToken}");

var heldStatus = await theLock.GetStatusAsync();
Console.WriteLine($"   status: held={heldStatus.IsHeld} queueDepth={heldStatus.QueueDepth}");

lease = await theLock.RenewAsync(lease.Token, TimeSpan.FromSeconds(30));
Console.WriteLine("   renewed the lease.");

await theLock.ReleaseAsync(lease.Token);
Console.WriteLine("   released. The next caller can acquire immediately.");
Console.WriteLine();

// --- 2. Non-blocking try-acquire under contention --------------------------
// TryAcquire never queues: it returns a lease if the lock is free right now, or
// null if it is currently held. Use it for a singleton job that should be skipped
// when another worker already holds the lock.
var holder = await theLock.AcquireAsync(new LockAcquireRequest(
    LeaseDuration: TimeSpan.FromSeconds(30),
    MaxWait: TimeSpan.FromSeconds(5)));
Console.WriteLine($"2) Holder acquired, fencing token = {holder.Token.FencingToken}.");

var contended = await theLock.TryAcquireAsync(TimeSpan.FromSeconds(30));
Console.WriteLine($"   TryAcquire while held -> {(contended is null ? "null (skipped)" : "granted")}");

await theLock.ReleaseAsync(holder.Token);
var afterRelease = await theLock.TryAcquireAsync(TimeSpan.FromSeconds(30));
Console.WriteLine($"   TryAcquire after release -> {(afterRelease is null ? "null" : $"granted, fencing token = {afterRelease.Value.Token.FencingToken}")}");
if (afterRelease is not null)
{
    await theLock.ReleaseAsync(afterRelease.Value.Token);
}
Console.WriteLine();

// --- 3. FIFO blocking: a queued waiter is granted on release ----------------
// A blocking AcquireAsync enqueues the caller FIFO and completes from a later turn
// - a release, a lease expiry, or its own wait-timeout - without ever blocking the
// grain's activation turn.
var first = await theLock.AcquireAsync(new LockAcquireRequest(
    LeaseDuration: TimeSpan.FromSeconds(30),
    MaxWait: TimeSpan.FromSeconds(5)));
Console.WriteLine($"3) First holder acquired, fencing token = {first.Token.FencingToken}.");

// A second caller starts waiting in the FIFO queue while the lock is held.
var waiter = theLock.AcquireAsync(new LockAcquireRequest(
    LeaseDuration: TimeSpan.FromSeconds(30),
    MaxWait: TimeSpan.FromSeconds(10)));
await Task.Delay(250); // let the waiter reach the queue
var queuedStatus = await theLock.GetStatusAsync();
Console.WriteLine($"   a second caller is queued: queueDepth={queuedStatus.QueueDepth}");

Console.WriteLine("   first holder releases...");
await theLock.ReleaseAsync(first.Token);

var second = await waiter; // completes the instant the release hands the lock over
Console.WriteLine($"   queued waiter granted, fencing token = {second.Token.FencingToken} (strictly higher).");
await theLock.ReleaseAsync(second.Token);
Console.WriteLine();

Console.WriteLine("Done.");
await host.StopAsync();

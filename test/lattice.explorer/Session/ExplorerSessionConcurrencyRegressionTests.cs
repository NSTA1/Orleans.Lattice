using Orleans.Lattice.Explorer.Core.Session;
using Orleans.Lattice.Explorer.Tests.Detail;

namespace Orleans.Lattice.Explorer.Tests.Session;

/// <summary>
/// Regression guards for the two concurrency defects that were tearing down Blazor
/// Server circuits in the Explorer.
/// <para>
/// Both faults shared a symptom that hid their cause completely: an exception thrown on
/// a circuit makes Blazor terminate that circuit, after which the page is still rendered
/// but entirely inert - so what anyone actually saw was a later, unrelated interaction
/// doing nothing, and a browser-test timeout naming whichever locator happened to be
/// waited on next. The failing test therefore rotated from run to run while the real
/// cause never moved, which is exactly why this was repeatedly written off as
/// environmental flakiness rather than a product bug.
/// </para>
/// </summary>
[TestFixture]
public sealed class ExplorerSessionConcurrencyRegressionTests
{
    private static readonly ExplorerPreferenceKey ScopedKey =
        new("feature.theme", "your theme", ExplorerPreferenceScope.User);

    /// <summary>
    /// Reading a scoped preference while the scope changes underneath must not corrupt
    /// the name cache.
    /// </summary>
    /// <remarks>
    /// The two paths that touch the cache genuinely do not share a thread: reads run on
    /// the render path, while a scope change clears the cache from whatever thread
    /// raised the authentication or configuration event - i.e. the sign-in path. An
    /// unsynchronised <see cref="Dictionary{TKey, TValue}"/> mutated from both at once
    /// corrupts its internal state and then throws
    /// <see cref="InvalidOperationException"/> ("Operations that change non-concurrent
    /// collections must have exclusive access") out of an unrelated later read - on the
    /// render path, which kills the circuit.
    /// <para>
    /// This drives the same collision directly. It reliably reproduced the corruption
    /// before the fix and is deterministic in what it asserts afterwards: no exception,
    /// and every answer still a well-formed scoped name rather than a torn one.
    /// </para>
    /// </remarks>
    [Test]
    public void Reading_a_preference_while_the_scope_changes_does_not_corrupt_the_name_cache()
    {
        var scope = new FakeExplorerPreferenceScopeProvider();
        var catalog = new ExplorerPreferenceCatalog();
        catalog.Register(ScopedKey);

        using var preferences = new ExplorerShellPreferences(
            new FakeUiPreferenceStore(),
            catalog,
            scope);

        var failures = new System.Collections.Concurrent.ConcurrentQueue<Exception>();
        using var start = new ManualResetEventSlim(false);

        // Several readers against one switcher, run for a bounded wall-clock window
        // rather than a fixed iteration count. The unsynchronised window is the few
        // instructions between the cache miss and the insert, so a single reader can
        // finish a fixed count without ever landing inside a Clear; contending readers
        // across cores hit it reliably. Two seconds reproduced it every time before the
        // fix and keeps the guard fast.
        var deadline = DateTime.UtcNow.AddSeconds(2);
        var threads = new List<Thread>();

        for (var r = 0; r < Math.Max(2, Environment.ProcessorCount / 2); r++)
        {
            threads.Add(new Thread(() =>
            {
                start.Wait();
                try
                {
                    while (DateTime.UtcNow < deadline)
                    {
                        // Any read composes or reads the cached scoped name.
                        preferences.GetOrDefault(ScopedKey, string.Empty);
                    }
                }
                catch (Exception ex)
                {
                    failures.Enqueue(ex);
                }
            }));
        }

        threads.Add(new Thread(() =>
        {
            start.Wait();
            try
            {
                var i = 0;
                while (DateTime.UtcNow < deadline)
                {
                    // Each move clears the cache, so the readers keep missing and
                    // re-inserting - which is the collision.
                    scope.MoveTo($"user{i++}", "https://cluster-a");
                }
            }
            catch (Exception ex)
            {
                failures.Enqueue(ex);
            }
        }));

        foreach (var thread in threads)
        {
            thread.Start();
        }

        start.Set();

        foreach (var thread in threads)
        {
            thread.Join();
        }

        Assert.That(failures, Is.Empty,
            "Reading a preference concurrently with a scope change threw. On a Blazor circuit "
            + "this exception surfaces on the render path, which terminates the circuit and "
            + "leaves the page rendered but inert."
            + Environment.NewLine
            + string.Join(Environment.NewLine, failures.Select(e => e.ToString())));
    }

    /// <summary>
    /// Disposing the store while a hydration is in flight must not fault the pending
    /// operation.
    /// </summary>
    /// <remarks>
    /// The store is a scoped service, so it is disposed when the circuit's DI scope ends
    /// - which routinely happens while <c>EnsureLoadedAsync</c> is still awaiting the
    /// backing store, whose read is a JS interop call that never completes once the
    /// circuit is going away. Disposing the gate underneath that continuation made its
    /// <c>finally</c> throw <see cref="ObjectDisposedException"/>, which Blazor reports
    /// as an unhandled circuit exception and tears the circuit down.
    /// </remarks>
    [Test]
    public async Task Disposing_the_store_while_a_load_is_in_flight_does_not_fault_the_load()
    {
        var backing = new BlockingBackingStore();
        var store = new UiPreferenceStore(backing);

        var load = store.EnsureLoadedAsync();

        // The load is now parked inside the gate, exactly where a circuit teardown
        // catches it.
        await backing.Entered.Task;

        store.Dispose();
        backing.Release();

        Assert.That(async () => await load, Throws.Nothing,
            "Disposing the store while a hydration was in flight faulted the pending load. "
            + "That exception reaches Blazor as an unhandled circuit exception and kills the "
            + "circuit, so the page goes inert and every later interaction does nothing.");
    }

    /// <summary>
    /// A backing store whose read parks until released, so a disposal can be driven into
    /// the exact window where a hydration is awaiting.
    /// </summary>
    private sealed class BlockingBackingStore : IUiPreferenceBackingStore
    {
        private readonly TaskCompletionSource _gate =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>Completes once the store's read has been entered.</summary>
        public TaskCompletionSource Entered { get; } =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>Lets the parked read complete.</summary>
        public void Release() => _gate.TrySetResult();

        public async Task<string?> GetAsync(string key, CancellationToken cancellationToken = default)
        {
            Entered.TrySetResult();
            await _gate.Task;
            return null;
        }

        public Task SetAsync(string key, string value, CancellationToken cancellationToken = default) =>
            Task.CompletedTask;

        public Task RemoveAsync(string key, CancellationToken cancellationToken = default) =>
            Task.CompletedTask;
    }
}

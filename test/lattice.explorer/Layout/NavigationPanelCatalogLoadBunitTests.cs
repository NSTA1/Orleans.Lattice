using Bunit;
using Microsoft.AspNetCore.Components;
using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using NUnit.Framework.Internal;
using Orleans.Lattice.Explorer.Core.Catalog;
using Orleans.Lattice.Explorer.Core.Configuration;
using Orleans.Lattice.Explorer.Core.Connection;
using Orleans.Lattice.Explorer.Core.DeadLetter;
using Orleans.Lattice.Explorer.Core.Session;
using Orleans.Lattice.Explorer.Tests.Bunit;
using Orleans.Lattice.Explorer.Tests.Detail;
using Orleans.Lattice.Explorer.UI.Layout;
using Orleans.Lattice.Explorer.UI.Pages;

namespace Orleans.Lattice.Explorer.Tests.Layout;

/// <summary>
/// <b>The catalog must reach its rows.</b> The shell mounts on a bare <c>/</c>
/// with nothing remembered - the ordinary first visit - and the listing appears,
/// whatever the surrounding services do while it is loading.
/// </summary>
/// <remarks>
/// <para>
/// This fixture exists because a defect shipped through a suite of 4307 green
/// tests. The catalog published its rows into <c>_items</c> and only rebuilt the
/// per-row badge buffers <em>after</em> awaiting a dead-letter count for every
/// tree. Any render landing in that window indexed a badge buffer shorter than
/// the row list and threw <see cref="IndexOutOfRangeException"/> straight out of
/// the render tree, which on a real circuit is fatal - the shell froze on
/// "Loading catalog..." and never navigated, never selected, and never opened a
/// dialog again.
/// </para>
/// <para>
/// <b>Why nothing caught it.</b> The window opens only when the catalog actually
/// returns rows and the kind is Trees; an empty catalog returns before the
/// fan-out, and almost every other fixture in this suite uses an empty one. It
/// also needed something to render during the window, which a genuinely
/// suspending preference store or a slow dead-letter reader supplies and an
/// instant in-memory double does not. Each axis below is one of those
/// conditions, held open on purpose.
/// </para>
/// <para>
/// Nothing here sleeps or polls on a wall clock: the settle wait is bUnit's
/// render-driven one, and it is present so that "never settles" is reported as a
/// failure rather than silently read as "not yet".
/// </para>
/// </remarks>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class NavigationPanelCatalogLoadBunitTests : LatticeComponentTestContext
{
    [Test]
    public void A_populated_catalog_renders_its_rows_on_a_bare_first_visit()
    {
        AssertCatalogRenders(new FakeUiPreferenceStore());
    }

    [TestCase(1, TestName = "hydrating_on_the_first_call")]
    [TestCase(2, TestName = "hydrating_on_the_second_call")]
    public void A_populated_catalog_renders_when_the_preference_store_actually_suspends(int hydrateOnCall)
    {
        // A real head reaches browser storage over JS interop, so EnsureLoadedAsync
        // genuinely gives up the thread and the panel's initialization overlaps its
        // own first render. An instant in-memory double never produces that
        // overlap, so it never renders during the load - and never caught this.
        AssertCatalogRenders(new SuspendingUiPreferenceStore { HydrateOnCall = hydrateOnCall });
    }

    [Test]
    public void A_populated_catalog_renders_even_when_the_dead_letter_reader_never_answers()
    {
        // The badge fan-out used to be awaited on the load path, and it is the one
        // awaited call that runs only for a catalog that returned rows. A reader
        // that never answers - as opposed to one that throws, which the per-tree
        // catch already handled - held the rows back indefinitely. Badges are
        // decoration; the listing is the content, and the content must not wait on
        // the decoration.
        var deadLetters = Substitute.For<IDeadLetterReader>();
        deadLetters
            .CountAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(_ => new TaskCompletionSource<int>().Task);

        AssertCatalogRenders(new FakeUiPreferenceStore(), deadLetters);
    }

    [Test]
    public void A_populated_catalog_renders_even_when_the_dead_letter_reader_faults()
    {
        var deadLetters = Substitute.For<IDeadLetterReader>();
        deadLetters
            .CountAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromException<int>(new InvalidOperationException("no dead-letter surface here")));

        AssertCatalogRenders(new FakeUiPreferenceStore(), deadLetters);
    }

    [Test]
    public void The_dead_letter_badges_still_arrive_once_a_slow_reader_answers()
    {
        // The fix must not turn "the rows wait for the badges" into "the badges
        // never come". The counts are now fetched behind the listing, so they have
        // to land on it when they do arrive.
        var release = new TaskCompletionSource<int>();
        var deadLetters = Substitute.For<IDeadLetterReader>();
        deadLetters
            .CountAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(_ => release.Task);

        var loads = Configure(new FakeUiPreferenceStore(), deadLetters);
        var cut = RenderShell();

        cut.WaitForState(() => cut.FindAll(".lx-shell-nav-item").Count > 0, TimeSpan.FromSeconds(5));

        Assert.That(
            cut.FindAll(".lx-badge").Any(badge => badge.TextContent.Contains("dead-letter", StringComparison.OrdinalIgnoreCase)),
            Is.False,
            "no count has arrived yet, so no dead-letter badge should be painted");

        release.SetResult(3);

        cut.WaitForState(
            () => cut.FindAll(".lx-badge").Any(badge =>
                badge.TextContent.Contains("dead-letter", StringComparison.OrdinalIgnoreCase)),
            TimeSpan.FromSeconds(5));

        Assert.That(
            cut.FindAll(".lx-badge").Select(badge => badge.TextContent),
            Has.Some.Contains("3"),
            $"the count arrived but was never folded into the badges (catalog reads={loads()})");
    }

    private void AssertCatalogRenders(IUiPreferenceStore preferences, IDeadLetterReader? deadLetters = null)
    {
        var loads = Configure(preferences, deadLetters);
        var cut = RenderShell();

        var settled = true;
        try
        {
            cut.WaitForState(() => cut.FindAll(".lx-shell-nav-item").Count > 0, TimeSpan.FromSeconds(5));
        }
        catch (Exception)
        {
            settled = false;
        }

        var state = cut.FindAll(".lx-shell-nav-state").Select(element => element.TextContent.Trim()).FirstOrDefault();

        Assert.Multiple(() =>
        {
            Assert.That(
                settled,
                Is.True,
                $"the catalog never reached its rows (state='{state}', catalog reads={loads()})");
            Assert.That(
                cut.FindAll(".lx-shell-nav-item").Select(element => element.TextContent),
                Has.Some.Contains("orders").And.Some.Contains("payments"));
        });
    }

    private Func<int> Configure(IUiPreferenceStore preferences, IDeadLetterReader? deadLetters)
    {
        // Registered before the shared shell services so these claim the slots.
        Services.AddSingleton(preferences);
        Services.AddSingleton<IExplorerSelection>(new ExplorerSelection());
        ConfigureShellServices();

        var connection = Substitute.For<ILatticeStateConnection>();
        connection.Status.Returns(LatticeConnectionStatus.Disconnected);

        var session = Substitute.For<IExplorerSession>();
        session.IsConfigured.Returns(true);

        var loads = 0;
        var catalog = Substitute.For<ICatalogReader>();
        catalog
            .LoadAsync(Arg.Any<CatalogKind>(), Arg.Any<string?>(), Arg.Any<int>(), Arg.Any<CancellationToken>())
            .Returns(call =>
            {
                loads++;

                // Completes synchronously, as a fast cluster does. That is the
                // shape that mis-sequences a "publish then await then rebuild"
                // load, because every continuation runs inline.
                return Task.FromResult(new CatalogPage
                {
                    Items =
                    [
                        new CatalogItem { Id = "orders", Kind = call.ArgAt<CatalogKind>(0) },
                        new CatalogItem { Id = "payments", Kind = call.ArgAt<CatalogKind>(0) },
                    ],
                });
            });

        Services.AddSingleton(connection);
        Services.AddSingleton(catalog);
        Services.AddSingleton(deadLetters ?? Substitute.For<IDeadLetterReader>());
        Services.AddSingleton(session);

        return () => loads;
    }

    private IRenderedComponent<MainLayout> RenderShell() =>
        Render<MainLayout>(parameters => parameters.Add(
            layout => layout.Body,
            (RenderFragment)(builder =>
            {
                builder.OpenComponent<Home>(0);
                builder.CloseComponent();
            })));

    /// <summary>
    /// A preference store whose hydration actually gives up the thread, the way a
    /// JS interop round trip to browser storage does.
    /// </summary>
    /// <remarks>
    /// The distinction is load-bearing rather than pedantic: with an instant
    /// double, a component's <c>OnInitializedAsync</c> runs to completion before
    /// its first render, so the two never interleave. A real head interleaves
    /// them, and this suite had no double that did - which is why a render landing
    /// mid-load went untested.
    /// </remarks>
    private sealed class SuspendingUiPreferenceStore : IUiPreferenceStore
    {
        private readonly Dictionary<string, object?> _values = new(StringComparer.Ordinal);
        private int _calls;

        public int HydrateOnCall { get; init; } = 1;

        public bool IsLoaded { get; private set; }

        public async Task EnsureLoadedAsync(CancellationToken cancellationToken = default)
        {
            await Task.Yield();

            _calls++;
            if (_calls >= HydrateOnCall)
            {
                IsLoaded = true;
            }
        }

        public bool TryGet<T>(string key, out T value)
        {
            if (_values.TryGetValue(key, out var stored) && stored is T typed)
            {
                value = typed;
                return true;
            }

            value = default!;
            return false;
        }

        public T GetOrDefault<T>(string key, T fallback = default!) =>
            TryGet<T>(key, out var value) ? value : fallback;

        public Task SetAsync<T>(
            string key,
            T value,
            string? owner = null,
            CancellationToken cancellationToken = default)
        {
            _values[key] = value;
            return Task.CompletedTask;
        }

        public Task RemoveAsync(string key, CancellationToken cancellationToken = default)
        {
            _values.Remove(key);
            return Task.CompletedTask;
        }

        public Task GarbageCollectAsync(
            IReadOnlyCollection<string> liveOwners,
            CancellationToken cancellationToken = default) => Task.CompletedTask;
    }
}

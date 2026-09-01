using Bunit;
using Bunit.TestDoubles;
using Microsoft.AspNetCore.Components;
using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using NUnit.Framework.Internal;
using Orleans.Lattice.Explorer.Core.Catalog;
using Orleans.Lattice.Explorer.Core.Configuration;
using Orleans.Lattice.Explorer.Core.Connection;
using Orleans.Lattice.Explorer.Core.DeadLetter;
using Orleans.Lattice.Explorer.Core.Navigation;
using Orleans.Lattice.Explorer.Core.Session;
using Orleans.Lattice.Explorer.Tests.Bunit;
using Orleans.Lattice.Explorer.UI.Authentication;
using Orleans.Lattice.Explorer.UI.Layout;
using Orleans.Lattice.Explorer.UI.Pages;

namespace Orleans.Lattice.Explorer.Tests.Layout;

/// <summary>
/// <b>First-render restore must not navigate, and must not destroy transient UI
/// state.</b> The shell mounts, restores where the user was, and settles - all
/// without asking the head to move, because the address already describes the
/// view being restored.
/// </summary>
/// <remarks>
/// <para>
/// <b>Every case here runs against a catalog that RETURNS ITEMS, and that is the
/// whole point of the fixture.</b> The rest of the suite renders the panel over
/// an empty or failed catalog, where the remembered selection never resolves to a
/// row, so the publish path barely fires and a defect in it is invisible. That
/// gap shipped a real regression: on a populated catalog the restore published a
/// route it was already on, which displaced the entry policy's buffered restore
/// and turned its history <em>replace</em> into a <em>push</em> - a genuine
/// navigation while the shell was still mounting, which on a real head discarded
/// a sign-in dialog the user had just opened. A green suite of empty-catalog
/// tests proved nothing about it.
/// </para>
/// <para>
/// The assertions read <see cref="BunitNavigationManager.History"/> rather than
/// rendered output, because "did the shell ask the browser to move?" is the
/// actual invariant.
/// </para>
/// <para>
/// <b>The catalog answers synchronously, deliberately.</b> An earlier draft
/// deferred it to model a read still in flight, and that draft was itself
/// unreliable: <c>Render</c> returns once the render pass completes, which is not
/// the same instant as the panel reaching its catalog call, so releasing the read
/// raced the code that asks for it. A test that has to guess when the subject is
/// ready is a test that will fail on somebody else's machine. The publication is
/// what did the damage, so the publication is what these cases drive - directly,
/// with no interleaving to guess at.
/// </para>
/// </remarks>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class ShellFirstRenderRestoreBunitTests : LatticeComponentTestContext
{
    private const string SignInButton = ".lx-shell-auth-signin";
    private const string Overlay = ".lx-shell-config-overlay";

    [Test]
    public void A_deep_link_restore_over_a_populated_catalog_performs_no_navigation()
    {
        Configure();
        Navigation.NavigateTo("/explore/trees/orders");
        var before = History.Count;

        RenderShell();

        Assert.Multiple(() =>
        {
            Assert.That(
                Selection.Selected?.Id,
                Is.EqualTo("orders"),
                "prove the restore actually resolved the row before asserting it stayed quiet");
            Assert.That(
                Since(before),
                Is.Empty,
                "the address already describes the view, so there is nothing to ask the head for");
        });
    }

    [Test]
    public void Restoring_a_remembered_view_replaces_the_history_entry_rather_than_pushing()
    {
        Configure();
        Remember("explore", "trees", "orders");
        var before = History.Count;

        RenderShell();

        var performed = Since(before);

        Assert.Multiple(() =>
        {
            Assert.That(Selection.Selected?.Id, Is.EqualTo("orders"));
            Assert.That(
                performed,
                Has.Length.EqualTo(1),
                "only the entry policy's restore; the catalog must not publish a route it is already on");
            Assert.That(
                performed[0].Options.ReplaceHistoryEntry,
                Is.True,
                "the user asked for '/', so Back must leave the Explorer rather than land back on '/'");
        });
    }

    [Test]
    public void Restoring_a_selection_the_address_already_names_asks_the_head_for_nothing()
    {
        // The history assertions above measure the OUTCOME, and they stay green
        // even without the publish guard, because the head separately refuses to
        // navigate to the address it is already on. This measures the CAUSE: the
        // shell does not ask at all, rather than asking and being refused.
        //
        // That distinction is the whole defect. A request raised before the
        // circuit is live is buffered, and the buffer keeps only the latest - so
        // an unnecessary request there does not merely get refused, it DISPLACES
        // a real one. Asserting only on the outcome would let the guard rot out
        // and the regression return by a slightly different route.
        Configure();
        Navigation.NavigateTo("/explore/trees/orders");

        var requests = 0;
        Services.GetRequiredService<IExplorerShellRouter>().NavigationRequested += _ => requests++;

        RenderShell();

        Assert.Multiple(() =>
        {
            Assert.That(Selection.Selected?.Id, Is.EqualTo("orders"), "the restore did resolve the row");
            Assert.That(requests, Is.Zero, "and asked the head for nothing while doing it");
        });
    }

    [Test]
    public void An_open_sign_in_dialog_survives_a_selection_the_shell_adopts_from_the_address()
    {
        // The regression's damage was done by the publication, so this drives the
        // publication with a dialog open: the caller opens sign-in, the address
        // then names a different row (a deep link followed, Back, or the restore
        // settling), and the shell adopts it. Nothing about that is the user
        // asking to navigate, so nothing may tear the dialog down.
        Configure();
        Navigation.NavigateTo("/explore/trees/orders");

        var cut = RenderShell();
        cut.Find(SignInButton).Click();

        Assert.That(
            cut.FindComponents<LoginDialog>(),
            Is.Not.Empty,
            "prove the dialog opened before asserting it survived");

        var before = History.Count;
        cut.InvokeAsync(() =>
                Services.GetRequiredService<IExplorerShellRouter>().SetAddress("/explore/trees/payments"))
            .GetAwaiter()
            .GetResult();

        Assert.Multiple(() =>
        {
            Assert.That(Selection.Selected?.Id, Is.EqualTo("payments"), "the shell did follow the address");
            Assert.That(
                Since(before),
                Is.Empty,
                "adopting an address must not turn round and ask the head to navigate again");
            Assert.That(
                cut.FindComponents<LoginDialog>(),
                Is.Not.Empty,
                "the dialog the caller opened is still open");
            Assert.That(cut.FindAll(Overlay), Is.Not.Empty, "and its overlay is still painted");
        });
    }

    [Test]
    public void A_selection_the_caller_actually_makes_still_reaches_the_address_bar()
    {
        // The guard suppresses only a publication that would change nothing. Over-
        // suppressing would be the same defect wearing the opposite sign: no deep
        // link, and nothing for Back to return to.
        Configure();
        Navigation.NavigateTo("/explore/trees/orders");

        var cut = RenderShell();
        var before = History.Count;

        cut.FindAll(".lx-shell-nav-item").First(item => item.TextContent.Contains("payments")).Click();

        var performed = Since(before);

        Assert.Multiple(() =>
        {
            Assert.That(performed, Has.Length.EqualTo(1));
            Assert.That(performed[0].Uri, Is.EqualTo("/explore/trees/payments"));
            Assert.That(
                performed[0].Options.ReplaceHistoryEntry,
                Is.False,
                "a navigation the caller asked for pushes, so Back returns them to the previous selection");
        });
    }

    [Test]
    public void An_address_adopted_while_the_catalog_is_still_loading_keeps_its_selection()
    {
        // On a real cluster the catalog read is not instant, so a restore's
        // address can arrive while the first read is still outstanding. The
        // pending selection has to survive that window: resolving it against a
        // list that has not arrived yet finds nothing, and consuming it there
        // silently drops the selection the link asked for - the shell then sits
        // on the right URL showing "Nothing selected", which is the very symptom
        // this whole change set exists to remove.
        //
        // The gate makes this deterministic without guessing at timing: the test
        // blocks until the panel has actually ASKED for the catalog (a
        // happens-before edge published by the subject itself), and only then
        // moves the address and answers the read.
        var gate = new GatedCatalogReader();
        Configure(gate);
        Navigation.NavigateTo("/explore/trees");

        var cut = RenderShell();
        gate.WaitUntilRequested();

        var router = Services.GetRequiredService<IExplorerShellRouter>();
        cut.InvokeAsync(() => router.SetAddress("/explore/trees/orders")).GetAwaiter().GetResult();

        gate.Release(cut);

        Assert.That(
            Selection.Selected?.Id,
            Is.EqualTo("orders"),
            "the address arrived mid-load, and the selection it named must still be applied");
    }

    private NavigationManager Navigation => Services.GetRequiredService<NavigationManager>();

    private IReadOnlyCollection<NavigationHistory> History =>
        ((BunitNavigationManager)Navigation).History;

    private IExplorerSelection Selection => Services.GetRequiredService<IExplorerSelection>();

    /// <summary>
    /// The navigations performed since <paramref name="before"/> entries had been
    /// recorded.
    /// </summary>
    /// <remarks>
    /// bUnit records history <em>newest first</em>, so the entries added since a
    /// mark are the ones at the FRONT, not the ones after it. Naming that here
    /// keeps every case from re-learning it - reading the wrong end silently
    /// asserts against an older navigation and passes for the wrong reason.
    /// </remarks>
    private NavigationHistory[] Since(int before) =>
        History.Take(History.Count - before).ToArray();

    private void Remember(string area, string kind, string selection)
    {
        var preferences = Services.GetRequiredService<IExplorerShellPreferences>();
        preferences.EnsureLoadedAsync().GetAwaiter().GetResult();
        preferences.SetAsync(ExplorerPreferenceKeys.ActiveArea, area).GetAwaiter().GetResult();
        preferences.SetAsync(ExplorerPreferenceKeys.CatalogKind, kind).GetAwaiter().GetResult();
        preferences.SetAsync(ExplorerPreferenceKeys.Selection, selection).GetAwaiter().GetResult();
    }

    private IRenderedComponent<MainLayout> RenderShell() =>
        Render<MainLayout>(parameters => parameters.Add(
            layout => layout.Body,
            (RenderFragment)(builder =>
            {
                builder.OpenComponent<Home>(0);
                builder.CloseComponent();
            })));

    private void Configure(ICatalogReader? reader = null)
    {
        // Registered before the shared shell services so this claims the slot;
        // the real selection type is used because its own change-suppression is
        // part of what is under test.
        Services.AddSingleton<IExplorerSelection>(new ExplorerSelection());
        ConfigureShellServices();

        var connection = Substitute.For<ILatticeStateConnection>();
        connection.Status.Returns(LatticeConnectionStatus.Disconnected);

        var session = Substitute.For<IExplorerSession>();
        session.IsConfigured.Returns(true);

        Services.AddSingleton(connection);
        Services.AddSingleton(reader ?? SynchronousCatalog());
        Services.AddSingleton(Substitute.For<IDeadLetterReader>());
        Services.AddSingleton(session);
    }

    private static ICatalogReader SynchronousCatalog()
    {
        var catalog = Substitute.For<ICatalogReader>();
        catalog
            .LoadAsync(Arg.Any<CatalogKind>(), Arg.Any<string?>(), Arg.Any<int>(), Arg.Any<CancellationToken>())
            .Returns(call => Task.FromResult(Page(call.ArgAt<CatalogKind>(0))));

        return catalog;
    }

    private static CatalogPage Page(CatalogKind kind) => new()
    {
        Items =
        [
            new CatalogItem { Id = "orders", Kind = kind },
            new CatalogItem { Id = "payments", Kind = kind },
        ],
    };

    /// <summary>
    /// A catalog that answers only when the test says so, and that publishes when
    /// it has been <em>asked</em> so the test never has to guess whether the
    /// subject has got there yet.
    /// </summary>
    /// <remarks>
    /// The signal is the whole point. <c>Render</c> returning is not the same
    /// instant as the panel reaching its catalog call, so releasing the read on
    /// that assumption races the code that asks for it - a test that guesses when
    /// its subject is ready is a test that fails on somebody else's machine.
    /// Waiting on an edge the subject itself publishes is synchronisation, not a
    /// delay: there is no timeout here and no polling, and a read that never
    /// arrives hangs the case rather than passing it by luck.
    /// <para>
    /// The completion source runs its continuations inline, so releasing a read
    /// resumes the panel on the renderer's dispatcher before <c>Release</c>
    /// returns.
    /// </para>
    /// </remarks>
    private sealed class GatedCatalogReader : ICatalogReader
    {
        private readonly TaskCompletionSource _requested = new();
        private readonly List<(TaskCompletionSource<CatalogPage> Source, CatalogKind Kind)> _pending = [];
        private readonly object _gate = new();

        public Task<CatalogPage> LoadAsync(
            CatalogKind kind,
            string? pageToken,
            int pageSize,
            CancellationToken cancellationToken = default)
        {
            var source = new TaskCompletionSource<CatalogPage>();

            lock (_gate)
            {
                _pending.Add((source, kind));
            }

            _requested.TrySetResult();
            return source.Task;
        }

        /// <summary>Blocks until the panel has asked for the catalog at least once.</summary>
        public void WaitUntilRequested() => _requested.Task.GetAwaiter().GetResult();

        /// <summary>Answers every outstanding read, on the renderer's dispatcher.</summary>
        public void Release(IRenderedComponent<MainLayout> cut) =>
            cut.InvokeAsync(() =>
            {
                (TaskCompletionSource<CatalogPage> Source, CatalogKind Kind)[] pending;
                lock (_gate)
                {
                    pending = _pending.ToArray();
                    _pending.Clear();
                }

                foreach (var (source, kind) in pending)
                {
                    source.SetResult(Page(kind));
                }
            }).GetAwaiter().GetResult();
    }
}

using Bunit;
using NSubstitute;
using Orleans.Lattice.Api.Telemetry;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Telemetry;

[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class TelemetryPageOwnershipTests : TelemetryTestContext
{
    [TestCase("latency")]
    [TestCase("nowhere")]
    public async Task A_catalogue_reply_resolves_the_current_board_not_the_address_that_started_it(string previous)
    {
        var pending = Telemetry.PendingCatalog = new TaskCompletionSource<TelemetryQueryCatalog>();
        var notFound = 0;
        Navigation.OnNotFound += (_, _) => notFound++;
        var cut = RenderPage("telemetry/" + previous);
        cut.WaitUntil(() => Assert.That(Telemetry.CatalogReads, Is.EqualTo(1)));

        await cut.InvokeAsync(() => Navigation.NavigateTo("telemetry/storage"));
        cut.Render();
        await cut.InvokeAsync(() => pending.SetResult(Telemetry.Catalog));

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find("h1").TextContent, Is.EqualTo("Storage"));
            Assert.That(notFound, Is.Zero);
            Assert.That(cut.FindAll(".lt-telemetry-chart"), Is.Not.Empty);
        });
    }

    [TestCase(false)]
    [TestCase(true)]
    public async Task A_superseded_catalogue_cannot_replace_the_latest_answer_or_publish_a_fault(bool fail)
    {
        var cut = RenderPage("telemetry");
        cut.WaitUntil(() => Assert.That(cut.FindAll("tbody tr"), Has.Count.EqualTo(4)));
        var pending = Telemetry.PendingCatalog = new TaskCompletionSource<TelemetryQueryCatalog>();
        await cut.InvokeAsync(CatalogCache.Invalidate);
        cut.WaitUntil(() => Assert.That(Telemetry.CatalogReads, Is.EqualTo(2)));
        var oldRead = CatalogCache.GetAsync();

        Telemetry.PendingCatalog = null;
        Telemetry.Catalog = TelemetryQueryCatalog.Empty;
        await cut.InvokeAsync(CatalogCache.Invalidate);
        cut.WaitUntil(() => Assert.That(cut.Find(".lt-empty h2").TextContent, Is.EqualTo("No metrics to show")));

        await cut.InvokeAsync(() =>
        {
            if (fail)
            {
                pending.SetException(new TelemetryBackendException(string.Empty, "old failure"));
            }
            else
            {
                pending.SetResult(TelemetryTestData.FullCatalog());
            }
        });
        if (fail)
        {
            Assert.ThrowsAsync<TelemetryBackendException>(async () => await oldRead);
        }
        else
        {
            await oldRead;
        }

        await cut.InvokeAsync(() => Task.CompletedTask);
        cut.Render();

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find(".lt-empty h2").TextContent, Is.EqualTo("No metrics to show"));
            Assert.That(cut.FindAll("table"), Is.Empty);
        });
    }

    [Test]
    public async Task A_superseded_catalogue_cannot_clear_the_latest_loading_state()
    {
        var old = Telemetry.PendingCatalog = new TaskCompletionSource<TelemetryQueryCatalog>();
        var cut = RenderPage("telemetry");
        cut.WaitUntil(() => Assert.That(Telemetry.CatalogReads, Is.EqualTo(1)));
        var oldRead = CatalogCache.GetAsync();
        var latest = Telemetry.PendingCatalog = new TaskCompletionSource<TelemetryQueryCatalog>();
        await cut.InvokeAsync(CatalogCache.Invalidate);
        cut.WaitUntil(() => Assert.That(Telemetry.CatalogReads, Is.EqualTo(2)));

        await cut.InvokeAsync(() => old.SetResult(TelemetryTestData.FullCatalog()));
        await oldRead;
        await cut.InvokeAsync(() => Task.CompletedTask);
        cut.Render();
        cut.WaitUntil(() => Assert.That(cut.FindAll(".lt-skeleton"), Has.Count.EqualTo(1)));
        await cut.InvokeAsync(() => latest.SetResult(TelemetryQueryCatalog.Empty));
        cut.WaitUntil(() => Assert.That(cut.Find(".lt-empty h2").TextContent, Is.EqualTo("No metrics to show")));
    }

    [TestCase(false, true)]
    [TestCase(true, false)]
    public async Task A_caller_change_rechecks_whether_the_operator_scope_choice_is_available(bool before, bool after)
    {
        UseTenancy("acme");
        Switcher!.IsOperatorAsync(Arg.Any<CancellationToken>()).Returns(new ValueTask<bool>(before));
        var cut = RenderPage("t/acme/telemetry/latency");
        cut.WaitUntil(() => Assert.That(cut.Find("h1").TextContent, Is.EqualTo("Latency")));
        Assert.That(cut.FindAll(".lt-telemetry-choice[aria-label='Tenant scope']").Count, Is.EqualTo(before ? 1 : 0));

        Switcher.IsOperatorAsync(Arg.Any<CancellationToken>()).Returns(new ValueTask<bool>(after));
        await cut.InvokeAsync(CatalogCache.Invalidate);

        cut.WaitUntil(() =>
        {
            Assert.That(Telemetry.CatalogReads, Is.EqualTo(2));
            Assert.That(cut.FindAll(".lt-telemetry-choice[aria-label='Tenant scope']").Count, Is.EqualTo(after ? 1 : 0));
        });
    }

    [TestCase(false)]
    [TestCase(true)]
    public async Task A_superseded_operator_probe_cannot_restore_the_previous_callers_controls(bool fail)
    {
        UseTenancy("acme");
        var old = new TaskCompletionSource<bool>();
        CancellationToken previous = default;
        Switcher!.IsOperatorAsync(Arg.Any<CancellationToken>()).Returns(call =>
        {
            previous = call.Arg<CancellationToken>();
            return new ValueTask<bool>(old.Task);
        });
        var cut = RenderPage("t/acme/telemetry/latency");
        cut.WaitUntil(() => Assert.That(previous.CanBeCanceled, Is.True));
        var oldCancellation = previous;

        try
        {
            Switcher.IsOperatorAsync(Arg.Any<CancellationToken>()).Returns(new ValueTask<bool>(false));
            await cut.InvokeAsync(CatalogCache.Invalidate);
            cut.WaitUntil(() => Assert.That(cut.Find("h1").TextContent, Is.EqualTo("Latency")));

            await cut.InvokeAsync(() =>
            {
                if (fail)
                {
                    old.SetException(new InvalidOperationException("old caller"));
                }
                else
                {
                    old.SetResult(true);
                }
            });
            Assert.That(oldCancellation.IsCancellationRequested, Is.True);
            Assert.That(cut.FindAll(".lt-telemetry-choice[aria-label='Tenant scope']"), Is.Empty);
        }
        finally
        {
            old.TrySetResult(false);
        }
    }
}

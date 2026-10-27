using Orleans.Lattice.Explorer.UI.Design.Components;

namespace Orleans.Lattice.Explorer.Tests.UI.Design.Components;

[TestFixture]
public sealed class LtSuggestionPumpTests
{
    [Test]
    public async Task Request_completed_queries_deliver_text_and_reuse_an_uncancelled_token()
    {
        var delivered = new List<(string Text, LtSuggestionSet Answer)>();
        var source = new FakeSuggestionSource("orders", "audit");
        using var pump = new LtSuggestionPump((text, answer) =>
        {
            delivered.Add((text, answer));
            return Task.CompletedTask;
        });

        Assert.That(pump.IsRunning, Is.False);
        await pump.RequestAsync(source, "o", 1);
        await pump.RequestAsync(source, "a", 3);

        Assert.Multiple(() =>
        {
            Assert.That(pump.IsRunning, Is.False);
            Assert.That(delivered.Select(item => item.Text), Is.EqualTo(new[] { "o", "a" }));
            Assert.That(delivered[0].Answer.Find("orders"), Is.Not.Null);
            Assert.That(delivered[1].Answer.Find("audit"), Is.Not.Null);
            Assert.That(source.Queries.Select(query => query.Limit), Is.EqualTo(new[] { 1, 3 }));
            Assert.That(source.Queries[1].Token, Is.EqualTo(source.Queries[0].Token));
            Assert.That(source.Queries[0].Token.IsCancellationRequested, Is.False);
        });
    }

    [Test]
    public async Task Request_while_running_coalesces_to_the_latest_source_text_and_limit()
    {
        var delivered = new List<string>();
        var first = new FakeSuggestionSource("old") { Gated = true, IgnoresCancellation = true };
        var latest = new FakeSuggestionSource("new");
        using var pump = new LtSuggestionPump((text, _) =>
        {
            delivered.Add(text);
            return Task.CompletedTask;
        });

        var running = pump.RequestAsync(first, "old", 1);
        Assert.That(pump.IsRunning, Is.True);
        var intermediate = pump.RequestAsync(first, "ignored", 2);
        var final = pump.RequestAsync(latest, "new", 7);
        Assert.Multiple(() =>
        {
            Assert.That(intermediate, Is.SameAs(running));
            Assert.That(final, Is.SameAs(running));
            Assert.That(first.Queries[0].Token.IsCancellationRequested, Is.True);
        });

        first.Queries[0].Gate.SetResult(first.Answer("old", 1));
        await running.WaitAsync(TimeSpan.FromSeconds(5));

        Assert.Multiple(() =>
        {
            Assert.That(pump.IsRunning, Is.False);
            Assert.That(delivered, Is.EqualTo(new[] { "new" }));
            Assert.That(first.Queries, Has.Count.EqualTo(1));
            Assert.That(latest.Queries.Single().Limit, Is.EqualTo(7));
            Assert.That(latest.Queries[0].Token, Is.Not.EqualTo(first.Queries[0].Token));
        });
    }

    [TestCase(false)]
    [TestCase(true)]
    public async Task Cancel_abandons_running_and_pending_answers_and_allows_a_fresh_request(bool observesCancellation)
    {
        var delivered = new List<string>();
        var source = new FakeSuggestionSource("orders") { Gated = true, IgnoresCancellation = !observesCancellation };
        using var pump = new LtSuggestionPump((text, _) =>
        {
            delivered.Add(text);
            return Task.CompletedTask;
        });

        var running = pump.RequestAsync(source, "old", 1);
        if (!observesCancellation)
        {
            _ = pump.RequestAsync(source, "pending", 2);
        }
        pump.Cancel();
        if (!observesCancellation)
        {
            source.Queries[0].Gate.SetResult(source.Answer("old", 1));
        }

        await running.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Multiple(() =>
        {
            Assert.That(delivered, Is.Empty);
            Assert.That(source.Queries, Has.Count.EqualTo(1));
            Assert.That(pump.IsRunning, Is.False);
        });

        source.Gated = false;
        await pump.RequestAsync(source, "orders", 3);
        Assert.That(delivered, Is.EqualTo(new[] { "orders" }));
    }

    [Test]
    public async Task Dispose_cancels_running_work_and_refuses_subsequent_requests()
    {
        var delivered = 0;
        var source = new FakeSuggestionSource { Gated = true, IgnoresCancellation = true };
        var pump = new LtSuggestionPump((_, _) =>
        {
            delivered++;
            return Task.CompletedTask;
        });
        var running = pump.RequestAsync(source, "old", 1);

        pump.Dispose();
        pump.Dispose();
        source.Queries[0].Gate.SetResult(source.Answer("old", 1));
        await running.WaitAsync(TimeSpan.FromSeconds(5));
        await pump.RequestAsync(source, "new", 2);

        Assert.Multiple(() =>
        {
            Assert.That(source.Queries[0].Token.IsCancellationRequested, Is.True);
            Assert.That(source.Queries, Has.Count.EqualTo(1));
            Assert.That(delivered, Is.Zero);
            Assert.That(pump.IsRunning, Is.False);
        });
    }

    [Test]
    public async Task Request_source_fault_delivers_an_unavailable_answer_then_recovers()
    {
        LtSuggestionSet? delivered = null;
        var source = new FakeSuggestionSource("orders") { Throws = new InvalidOperationException("private detail") };
        using var pump = new LtSuggestionPump((_, answer) =>
        {
            delivered = answer;
            return Task.CompletedTask;
        });

        await pump.RequestAsync(source, "orders", 1);
        Assert.Multiple(() =>
        {
            Assert.That(delivered, Is.Not.Null);
            Assert.That(delivered!.IsAvailable, Is.False);
            Assert.That(delivered.UnavailableReason, Is.EqualTo(LtSuggestionPump.FaultReason));
        });

        source.Throws = null;
        await pump.RequestAsync(source, "orders", 1);
        Assert.That(delivered!.Find("orders"), Is.Not.Null);
    }

    [Test]
    public void Request_null_arguments_are_rejected()
    {
        using var pump = new LtSuggestionPump((_, _) => Task.CompletedTask);
        Assert.Multiple(() =>
        {
            Assert.That(() => pump.RequestAsync(null!, "text", 1), Throws.ArgumentNullException.With.Property("ParamName").EqualTo("source"));
            Assert.That(() => pump.RequestAsync(new FakeSuggestionSource(), null!, 1), Throws.ArgumentNullException.With.Property("ParamName").EqualTo("text"));
        });
    }
}

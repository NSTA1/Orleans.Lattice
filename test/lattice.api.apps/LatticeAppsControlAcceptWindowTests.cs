using NSubstitute;
using Orleans.Lattice.Apps;
using Orleans.Lattice.Testing;

namespace Orleans.Lattice.Api.Apps.Tests;

/// <summary>
/// Issue #5482: an activation verb waits for its run only for the facade's accept window. A run
/// that outlasts it keeps running to completion and the caller gets a pending result, and the
/// caller's cancellation never stops the run part-way.
/// </summary>
[TestFixture]
public sealed class LatticeAppsControlAcceptWindowTests
{
    private AppsControlHarness _h = null!;

    [SetUp]
    public void SetUp() => _h = new AppsControlHarness();

    public enum Verb
    {
        Enable,
        Disable,
        Uninstall,
    }

    [TestCase(Verb.Enable, AppRegistryLifecycleState.Disabled, AppLifecycleState.Disabled)]
    [TestCase(Verb.Disable, AppRegistryLifecycleState.Enabled, AppLifecycleState.Enabled)]
    [TestCase(Verb.Uninstall, AppRegistryLifecycleState.Disabled, AppLifecycleState.Disabled)]
    public async Task A_run_that_outlasts_the_accept_window_returns_pending_with_the_prior_state(
        Verb verb, AppRegistryLifecycleState prior, AppLifecycleState expected)
    {
        _h.RegistryHas(AppsControlHarness.Record(prior));
        var run = Hold(verb, out var tokens);
        var control = _h.Control;

        var call = Invoke(control, verb, CancellationToken.None);
        await TestPoll.UntilAsync(() => _h.Time.PendingTimerCount > 0, "the facade to arm its accept window");
        _h.Time.Advance(control.AcceptWindow);
        var result = await call;

        Assert.Multiple(() =>
        {
            Assert.That(result.Pending, Is.True);
            Assert.That(result.State, Is.EqualTo(expected));
            Assert.That(result.Changed, Is.False);
            Assert.That(result.Version, Is.EqualTo(AppsControlHarness.Version));
            Assert.That(run.Task.IsCompleted, Is.False, "the run is still in flight");
            Assert.That(tokens.Single().CanBeCanceled, Is.False, "the run is not tied to the caller's cancellation");
        });

        run.SetResult(AppsControlHarness.Outcome(AppActivationOperation.Uninstall, AppRegistryLifecycleState.Uninstalled));
    }

    [Test]
    public async Task A_run_that_finishes_inside_the_accept_window_returns_its_result()
    {
        var run = Hold(Verb.Uninstall, out _);
        var control = _h.Control;

        var call = control.UninstallAsync(AppsControlHarness.Slug);
        await TestPoll.UntilAsync(() => _h.Time.PendingTimerCount > 0, "the facade to arm its accept window");
        run.SetResult(AppsControlHarness.Outcome(AppActivationOperation.Uninstall, AppRegistryLifecycleState.Uninstalled));
        var result = await call;

        Assert.Multiple(() =>
        {
            Assert.That(result.Pending, Is.False);
            Assert.That(result.State, Is.EqualTo(AppLifecycleState.Uninstalled));
            Assert.That(result.Changed, Is.True);
        });
        await _h.Registry.DidNotReceiveWithAnyArgs().GetAsync(default, default!, default);
    }

    [Test]
    public async Task The_caller_cancelling_stops_its_wait_but_not_the_run()
    {
        var run = Hold(Verb.Uninstall, out var tokens);
        using var cts = new CancellationTokenSource();

        var call = _h.Control.UninstallAsync(AppsControlHarness.Slug, cts.Token);
        await TestPoll.UntilAsync(() => _h.Time.PendingTimerCount > 0, "the facade to arm its accept window");
        await cts.CancelAsync();

        Assert.CatchAsync<OperationCanceledException>(async () => await call);
        Assert.Multiple(() =>
        {
            Assert.That(run.Task.IsCompleted, Is.False, "the run keeps going");
            Assert.That(tokens.Single().CanBeCanceled, Is.False);
        });

        run.SetResult(AppsControlHarness.Outcome(AppActivationOperation.Uninstall, AppRegistryLifecycleState.Uninstalled));
    }

    [Test]
    public async Task A_pending_run_whose_app_record_is_gone_reports_not_found()
    {
        _h.RegistryHas(null);
        var run = Hold(Verb.Uninstall, out _);
        var control = _h.Control;

        var call = control.UninstallAsync(AppsControlHarness.Slug);
        await TestPoll.UntilAsync(() => _h.Time.PendingTimerCount > 0, "the facade to arm its accept window");
        _h.Time.Advance(control.AcceptWindow);

        Assert.ThrowsAsync<KeyNotFoundException>(async () => await call);
        run.SetResult(AppsControlHarness.Outcome(AppActivationOperation.Uninstall, AppRegistryLifecycleState.Uninstalled));
    }

    [Test]
    public void The_default_accept_window_is_under_every_ingress_request_timeout()
    {
        // The reference Front Door allows 60 s and a default YARP route 100 s.
        Assert.That(LatticeAppsControl.DefaultAcceptWindow, Is.LessThan(TimeSpan.FromSeconds(60)));
        Assert.That(_h.Control.AcceptWindow, Is.EqualTo(LatticeAppsControl.DefaultAcceptWindow));
    }

    private TaskCompletionSource<AppActivationOutcome> Hold(Verb verb, out List<CancellationToken> tokens)
    {
        var run = new TaskCompletionSource<AppActivationOutcome>(TaskCreationOptions.RunContinuationsAsynchronously);
        var seen = new List<CancellationToken>();
        tokens = seen;
        Func<NSubstitute.Core.CallInfo, Task<AppActivationOutcome>> answer = info =>
        {
            seen.Add(info.ArgAt<CancellationToken>(2));
            return run.Task;
        };

        _ = verb switch
        {
            Verb.Enable => _h.Pipeline.EnableAsync(Arg.Any<TenantId>(), Arg.Any<AppSlug>(), Arg.Any<CancellationToken>()).Returns(answer),
            Verb.Disable => _h.Pipeline.DisableAsync(Arg.Any<TenantId>(), Arg.Any<AppSlug>(), Arg.Any<CancellationToken>()).Returns(answer),
            _ => _h.Pipeline.UninstallAsync(Arg.Any<TenantId>(), Arg.Any<AppSlug>(), Arg.Any<CancellationToken>()).Returns(answer),
        };
        return run;
    }

    private static Task<AppLifecycleResult> Invoke(LatticeAppsControl control, Verb verb, CancellationToken cancellationToken) => verb switch
    {
        Verb.Enable => control.EnableAsync(AppsControlHarness.Slug, cancellationToken),
        Verb.Disable => control.DisableAsync(AppsControlHarness.Slug, cancellationToken),
        _ => control.UninstallAsync(AppsControlHarness.Slug, cancellationToken),
    };
}

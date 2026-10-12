using NSubstitute;
using Orleans.Lattice.Api.TenantAdmin;
using Orleans.Lattice.Explorer.UI.Areas.Access;
using Orleans.Lattice.Explorer.UI.Areas.Access.Tenant;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Access.Tenant;

[TestFixture]
public sealed class TenantAccessReadRaceTests
{
    private ILatticeTenantPolicyAdmin _policy = null!;
    private ILatticeTenantDirectoryAdmin _directory = null!;
    private TenantAccessCatalog _catalog = null!;

    [SetUp]
    public void SetUp()
    {
        _policy = Substitute.For<ILatticeTenantPolicyAdmin>();
        _directory = Substitute.For<ILatticeTenantDirectoryAdmin>();
        var facades = new FakeTenantAccessFacades { PolicyOverride = _policy, DirectoryOverride = _directory };
        _catalog = new TenantAccessCatalog(facades);
    }

    [Test]
    public async Task ResolveAsync_same_scope_waits_for_the_pending_posture()
    {
        var reply = new TaskCompletionSource<TenantAccessPosture>(TaskCreationOptions.RunContinuationsAsynchronously);
        _policy.GetPostureAsync("acme", Arg.Any<CancellationToken>()).Returns(reply.Task);
        var gate = new AccessTenantGate();

        var first = gate.ResolveAsync(_catalog, "acme");
        var second = gate.ResolveAsync(_catalog, "acme");
        var completedEarly = second.IsCompleted;
        reply.SetResult(Posture("acme"));
        var results = await Task.WhenAll(first, second);

        Assert.Multiple(() =>
        {
            Assert.That(completedEarly, Is.False, "A pending posture is not evidence that delegation is off.");
            Assert.That(results, Is.EqualTo(new[] { true, true }));
        });
        await _policy.Received(1).GetPostureAsync("acme", Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task ResolveAsync_obsolete_scope_does_not_end_the_current_loading_state()
    {
        var oldReply = new TaskCompletionSource<TenantAccessPosture>(TaskCreationOptions.RunContinuationsAsynchronously);
        var newReply = new TaskCompletionSource<TenantAccessPosture>(TaskCreationOptions.RunContinuationsAsynchronously);
        _policy.GetPostureAsync("acme", Arg.Any<CancellationToken>()).Returns(oldReply.Task);
        _policy.GetPostureAsync("globex", Arg.Any<CancellationToken>()).Returns(newReply.Task);
        var gate = new AccessTenantGate();

        var oldRead = gate.ResolveAsync(_catalog, "acme");
        var newRead = gate.ResolveAsync(_catalog, "globex");
        oldReply.SetResult(Posture("acme"));
        await oldRead;
        var stillResolving = gate.Resolving;
        newReply.SetResult(Posture("globex"));
        await newRead;

        Assert.Multiple(() =>
        {
            Assert.That(stillResolving, Is.True);
            Assert.That(gate.DelegatedTenant, Is.EqualTo("globex"));
            Assert.That(gate.Resolving, Is.False);
        });
    }

    [Test]
    public async Task ResolveAsync_cancelled_posture_can_be_retried_for_the_same_scope()
    {
        _policy.GetPostureAsync("acme", Arg.Any<CancellationToken>())
            .Returns(Task.FromCanceled<TenantAccessPosture>(new CancellationToken(true)), Task.FromResult(Posture("acme")));
        var gate = new AccessTenantGate();

        Assert.That(async () => await gate.ResolveAsync(_catalog, "acme"), Throws.InstanceOf<OperationCanceledException>());

        Assert.That(await gate.ResolveAsync(_catalog, "acme"), Is.True);
    }

    [Test]
    public async Task ResolveAsync_returning_to_a_scope_does_not_accept_its_obsolete_posture()
    {
        var oldReply = new TaskCompletionSource<TenantAccessPosture>(TaskCreationOptions.RunContinuationsAsynchronously);
        var newReply = new TaskCompletionSource<TenantAccessPosture>(TaskCreationOptions.RunContinuationsAsynchronously);
        _policy.GetPostureAsync("acme", Arg.Any<CancellationToken>()).Returns(oldReply.Task, newReply.Task);
        var gate = new AccessTenantGate();

        var oldRead = gate.ResolveAsync(_catalog, "acme");
        await gate.ResolveAsync(_catalog, null);
        var newRead = gate.ResolveAsync(_catalog, "acme");
        oldReply.SetResult(Posture("acme"));
        await oldRead;
        var stillResolving = gate.Resolving;
        var obsoleteState = gate.State;
        newReply.SetResult(Posture("acme", enabled: false));
        await newRead;

        Assert.Multiple(() =>
        {
            Assert.That(stillResolving, Is.True);
            Assert.That(obsoleteState, Is.Null);
            Assert.That(gate.DelegatedTenant, Is.Null);
            Assert.That(gate.State!.Standing, Is.EqualTo(TenantAccessStanding.Off));
        });
    }

    [Test]
    public async Task ResolveAsync_cancelled_read_rejects_a_reply_that_ignored_cancellation()
    {
        var reply = new TaskCompletionSource<TenantAccessPosture>(TaskCreationOptions.RunContinuationsAsynchronously);
        _policy.GetPostureAsync("acme", Arg.Any<CancellationToken>()).Returns(reply.Task);
        var gate = new AccessTenantGate();
        using var cancellation = new CancellationTokenSource();

        var read = gate.ResolveAsync(_catalog, "acme", cancellation.Token);
        cancellation.Cancel();
        reply.SetResult(Posture("acme"));

        Assert.That(async () => await read, Throws.InstanceOf<OperationCanceledException>());
        Assert.Multiple(() =>
        {
            Assert.That(gate.State, Is.Null);
            Assert.That(gate.Resolving, Is.False);
        });
        Assert.That(await gate.ResolveAsync(_catalog, "acme"), Is.True);
    }

    [Test]
    public async Task GetStateAsync_invalidated_pending_posture_is_not_remembered()
    {
        var reply = new TaskCompletionSource<TenantAccessPosture>(TaskCreationOptions.RunContinuationsAsynchronously);
        _policy.GetPostureAsync("acme", Arg.Any<CancellationToken>())
            .Returns(reply.Task, Task.FromResult(Posture("acme", enabled: false)));

        var oldRead = _catalog.GetStateAsync("acme", CancellationToken.None).AsTask();
        _catalog.Invalidate();
        var fresh = await _catalog.GetStateAsync("acme", CancellationToken.None);
        reply.SetResult(Posture("acme"));
        await oldRead;

        Assert.That(await _catalog.GetStateAsync("acme", CancellationToken.None), Is.SameAs(fresh));
        await _policy.Received(2).GetPostureAsync("acme", Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task GetGroupsAsync_invalidated_pending_groups_are_not_remembered()
    {
        var reply = new TaskCompletionSource<TenantGroupPage>(TaskCreationOptions.RunContinuationsAsynchronously);
        _directory.ListGroupsAsync("acme", Arg.Any<TenantAccessPageRequest>(), Arg.Any<CancellationToken>())
            .Returns(reply.Task, Task.FromResult(new TenantGroupPage { Entries = [new TenantGroupDescriptor { Name = "new" }] }));

        var oldRead = _catalog.GetGroupsAsync("acme", CancellationToken.None);
        _catalog.Invalidate();
        var fresh = await _catalog.GetGroupsAsync("acme", CancellationToken.None);
        reply.SetResult(new TenantGroupPage { Entries = [new TenantGroupDescriptor { Name = "old" }] });
        await oldRead;

        Assert.That(await _catalog.GetGroupsAsync("acme", CancellationToken.None), Is.SameAs(fresh));
        await _directory.Received(2).ListGroupsAsync("acme", Arg.Any<TenantAccessPageRequest>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task GetRulesAsync_invalidated_pending_rules_are_not_remembered()
    {
        var reply = new TaskCompletionSource<TenantRulePage>(TaskCreationOptions.RunContinuationsAsynchronously);
        _policy.ListRulesAsync("acme", Arg.Any<TenantAccessPageRequest>(), Arg.Any<CancellationToken>())
            .Returns(reply.Task, Task.FromResult(new TenantRulePage { Entries = [new TenantRuleView { RuleId = "new" }] }));

        var oldRead = _catalog.GetRulesAsync("acme", CancellationToken.None);
        _catalog.Invalidate();
        var fresh = await _catalog.GetRulesAsync("acme", CancellationToken.None);
        reply.SetResult(new TenantRulePage { Entries = [new TenantRuleView { RuleId = "old" }] });
        await oldRead;

        Assert.That(await _catalog.GetRulesAsync("acme", CancellationToken.None), Is.SameAs(fresh));
        await _policy.Received(2).ListRulesAsync("acme", Arg.Any<TenantAccessPageRequest>(), Arg.Any<CancellationToken>());
    }

    private static TenantAccessPosture Posture(string tenant, bool enabled = true) =>
        new() { TenantId = tenant, Enabled = enabled, CallerIsTenantAdmin = true };
}

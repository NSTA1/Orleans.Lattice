namespace Orleans.Lattice.Auth.Tests;

/// <summary>
/// Tests for the gate's use of <see cref="ITenantGateEnforcer.EnforceAsync"/>
/// (issue #4001). The tenancy enforcer confirms a cross-tenant grant against the
/// tenant registry while its compiled snapshot is being rebuilt, which completes
/// asynchronously, so both of the gate's enforcement points - the authorization
/// decision and the read-grant existence probe - must await it. These tests drive
/// the gate with an enforcer whose asynchronous decision is held on a gate and
/// whose synchronous <see cref="ITenantGateEnforcer.Enforce"/> answers the
/// opposite way, so a gate that consulted the synchronous form, or composed the
/// asynchronous result incorrectly, is caught.
/// </summary>
[TestFixture]
public sealed class PolicyAccessGateAsyncTenantEnforcementTests
{
    /// <summary>
    /// An enforcer whose <see cref="EnforceAsync"/> completes only when released,
    /// and whose synchronous <see cref="Enforce"/> returns the opposite decision.
    /// </summary>
    private sealed class HeldTenantGateEnforcer(bool asyncAllows) : ITenantGateEnforcer
    {
        private readonly TaskCompletionSource<LatticeAccessDecision> _held =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        public int SyncCalls { get; private set; }

        public int AsyncCalls { get; private set; }

        public bool IsActive => true;

        public LatticeAccessDecision Enforce(in LatticeAccessRequest request)
        {
            SyncCalls++;
            return asyncAllows
                ? LatticeAccessDecision.Deny("synchronous form denies")
                : LatticeAccessDecision.Allow();
        }

        public ValueTask<LatticeAccessDecision> EnforceAsync(
            in LatticeAccessRequest request,
            CancellationToken cancellationToken = default)
        {
            AsyncCalls++;
            return new ValueTask<LatticeAccessDecision>(_held.Task);
        }

        public void Release() =>
            _held.TrySetResult(asyncAllows
                ? LatticeAccessDecision.Allow()
                : LatticeAccessDecision.Deny("grant revoked; confirmed against the registry"));
    }

    /// <summary>An enforcer implementing only the synchronous member, to exercise the default <c>EnforceAsync</c>.</summary>
    private sealed class SyncOnlyTenantGateEnforcer(LatticeAccessDecision decision) : ITenantGateEnforcer
    {
        public bool IsActive => true;

        public LatticeAccessDecision Enforce(in LatticeAccessRequest request) => decision;
    }

    private static LatticeAccessRequest CrossTenantRead() =>
        new("t/acme/orders", LatticeOperation.Read, new LatticeSubject("bob"), "k");

    private static Task<AuthGateHarness.Harness> AllowingHarnessAsync(ITenantGateEnforcer enforcer) =>
        AuthGateHarness.CreateAsync(new LatticeAuthOptions { DefaultEffect = LatticeEffect.Allow }, enforcer);

    [Test]
    public async Task AuthorizeAsync_awaits_an_asynchronous_tenant_deny_and_it_overrides_a_policy_allow()
    {
        var enforcer = new HeldTenantGateEnforcer(asyncAllows: false);
        var harness = await AllowingHarnessAsync(enforcer);

        var pending = harness.Gate.AuthorizeAsync(CrossTenantRead());
        Assert.That(pending.IsCompleted, Is.False, "the gate awaits the enforcer's confirmation");
        enforcer.Release();
        var decision = await pending;

        Assert.Multiple(() =>
        {
            Assert.That(decision.Allowed, Is.False, "a confirmed tenant deny denies");
            Assert.That(decision.Reason, Does.Contain("revoked"));
            Assert.That(enforcer.SyncCalls, Is.Zero, "the gate consults EnforceAsync, never the synchronous form");
        });
    }

    [Test]
    public async Task AuthorizeAsync_awaits_an_asynchronous_tenant_allow_and_keeps_the_policy_allow()
    {
        var enforcer = new HeldTenantGateEnforcer(asyncAllows: true);
        var harness = await AllowingHarnessAsync(enforcer);

        var pending = harness.Gate.AuthorizeAsync(CrossTenantRead());
        enforcer.Release();
        var decision = await pending;

        Assert.Multiple(() =>
        {
            Assert.That(decision.Allowed, Is.True, "a confirmed grant is admitted (read-your-writes)");
            Assert.That(enforcer.AsyncCalls, Is.EqualTo(1));
            Assert.That(enforcer.SyncCalls, Is.Zero, "the synchronous form would have denied");
        });
    }

    [Test]
    public async Task HasAnyGrantAsync_awaits_an_asynchronous_tenant_deny_and_hides_the_tree()
    {
        var enforcer = new HeldTenantGateEnforcer(asyncAllows: false);
        var harness = await AllowingHarnessAsync(enforcer);

        var pending = harness.Gate.HasAnyGrantAsync("t/acme/orders", new LatticeSubject("bob"), LatticeOperation.Read);
        Assert.That(pending.IsCompleted, Is.False, "the probe awaits the enforcer's confirmation");
        enforcer.Release();

        Assert.That(await pending, Is.False, "a probe never out-reaches the enforcement decision");
        Assert.That(enforcer.SyncCalls, Is.Zero);
    }

    [Test]
    public async Task HasAnyGrantAsync_awaits_an_asynchronous_tenant_allow_and_reveals_the_tree()
    {
        var enforcer = new HeldTenantGateEnforcer(asyncAllows: true);
        var harness = await AllowingHarnessAsync(enforcer);

        var pending = harness.Gate.HasAnyGrantAsync("t/acme/orders", new LatticeSubject("bob"), LatticeOperation.Read);
        enforcer.Release();

        Assert.That(await pending, Is.True);
        Assert.That(enforcer.SyncCalls, Is.Zero);
    }

    [Test]
    public async Task EnforceAsync_default_implementation_completes_synchronously_with_the_Enforce_decision()
    {
        ITenantGateEnforcer enforcer = new SyncOnlyTenantGateEnforcer(LatticeAccessDecision.Deny("fixed"));
        var request = CrossTenantRead();

        var pending = enforcer.EnforceAsync(in request);

        Assert.That(pending.IsCompletedSuccessfully, Is.True, "the default never allocates a continuation");
        var decision = await pending;
        Assert.Multiple(() =>
        {
            Assert.That(decision.Allowed, Is.False);
            Assert.That(decision.Reason, Is.EqualTo("fixed"));
        });
    }

    [Test]
    public async Task AuthorizeAsync_with_a_synchronous_only_enforcer_composes_its_deny()
    {
        var harness = await AllowingHarnessAsync(new SyncOnlyTenantGateEnforcer(LatticeAccessDecision.Deny("sync deny")));

        var pending = harness.Gate.AuthorizeAsync(CrossTenantRead());

        Assert.That(pending.IsCompletedSuccessfully, Is.True, "a synchronous enforcer keeps the gate synchronous");
        Assert.That((await pending).Reason, Is.EqualTo("sync deny"));
    }
}

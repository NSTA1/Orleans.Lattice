using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using Orleans.Lattice.Auth;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Apps.Tests;

/// <summary>
/// The remaining cold arms across the activation, ownership, registry, and
/// provisioning collaborators: a registry transition refused rather than thrown,
/// cancellation propagating out of a run, an approved exception whose kind the
/// compiler does not recognise, a manifest carrying a null tree, a duplicate role
/// binding, and soft-deleting a tree that is not registered.
/// </summary>
[TestFixture]
public sealed class AppActivationRefusalArmTests
{
    private const LatticeScopeKind UnknownScopeKind = (LatticeScopeKind)99;

    // ----- a refused (not thrown) registry transition -----

    [Test]
    public async Task Enable_whose_transition_is_refused_by_a_concurrent_write_withdraws_the_rules()
    {
        // A refusal is not a fault: the registry returns a structured error rather
        // than throwing, and the engine must still not leave the compiled rules live
        // behind a record that was never enabled. This is the arm a concurrent
        // consent write reaches in production, and it is entirely separate from the
        // throwing path.
        var harness = new ActivationHarness();
        await harness.InstallAsync(ActivationHarness.Manifest());

        var key = AppRegistryTreeNames.ComposeKey(TenantId.Default, ActivationHarness.Slug);
        var stored = harness.RegistryStore.Peek(key)!;

        // Re-seed the key on every write attempt, so the conditional write's expected
        // version is always stale and every retry is refused.
        harness.RegistryStore.BeforeSet = k =>
        {
            if (string.Equals(k, key, StringComparison.Ordinal))
            {
                harness.RegistryStore.Seed(k, stored);
            }
        };

        var outcome = await harness.RunAsync(AppActivationOperation.Enable);

        Assert.Multiple(() =>
        {
            Assert.That(outcome.Succeeded, Is.False);
            Assert.That(outcome.Failure, Is.EqualTo(AppActivationFailure.RegistryConflict),
                "a lost conditional write must map onto a registry conflict, not a generic invalid transition");
            Assert.That(outcome.Diagnostics.Single().Code, Is.EqualTo("registry"));
            Assert.That(harness.OwnedRuleIds(), Is.Empty,
                "rules must not stay live behind a record the transition did not enable");
        });

        // Positive control: the same enable without the interposed writer succeeds
        // and does leave rules live.
        harness.RegistryStore.BeforeSet = null;
        var okOutcome = await harness.RunAsync(AppActivationOperation.Enable);
        Assert.Multiple(() =>
        {
            Assert.That(okOutcome.Succeeded, Is.True);
            Assert.That(harness.OwnedRuleIds(), Is.Not.Empty);
        });
    }

    // ----- cancellation is propagated, never recorded as a failure -----

    [Test]
    public void A_cancelled_activation_propagates_the_cancellation_rather_than_recording_a_failure()
    {
        // Cancellation is the caller's own decision, not the app's fault. Recording
        // it as a Faulted outcome would write a durable failure for an operation
        // nobody ever completed, and the next reconcile would read it as the app's
        // state.
        var harness = new ActivationHarness();
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        Assert.That(
            async () => await harness.Engine.ExecuteAsync(
                AppActivationOperation.Enable, TenantId.Default, ActivationHarness.Slug, cts.Token),
            Throws.InstanceOf<OperationCanceledException>());

        Assert.That(harness.Status.Writes, Is.Zero,
            "a cancelled run must not record an outcome");
    }

    // ----- an approved exception the compiler does not recognise -----

    [Test]
    public async Task An_approved_exception_of_an_unknown_kind_does_not_widen_the_ceiling()
    {
        // The compiler's own coverage check is the gate that decides whether a
        // foreign scope is consented. An unrecognised exception kind must cover
        // nothing, or a record written by a newer build would silently approve a
        // scope this build cannot even interpret.
        var harness = new ActivationHarness();
        var manifest = ActivationHarness.Manifest(
            trees: new[] { ActivationHarness.Tree("records"), ActivationHarness.Tree("legacy", adopted: "legacy-tree") },
            roles: new[] { ActivationHarness.Role("reader", LatticeOperation.Read, "legacy") });
        await harness.InstallAsync(
            manifest,
            ceiling: AppCapabilityCeiling.Structural(LatticeOperation.Read) with
            {
                ApprovedExceptionScopes = [new LatticeScope(UnknownScopeKind, "legacy-tree")],
            });

        var outcome = await harness.RunAsync(AppActivationOperation.Enable);

        Assert.Multiple(() =>
        {
            Assert.That(outcome.Failure, Is.EqualTo(AppActivationFailure.CeilingExceeded),
                "an exception of an unrecognised kind must not approve the scope");
            Assert.That(outcome.Diagnostics.Single().Code, Is.EqualTo("ceiling-scope"));
        });

        // Positive control: the same scope with a recognised tree exception is
        // approved, so the refusal above is the kind and not the scope.
        var ok = new ActivationHarness();
        await ok.InstallAsync(
            manifest,
            ceiling: AppCapabilityCeiling.Structural(LatticeOperation.Read) with
            {
                ApprovedExceptionScopes = [new LatticeScope(LatticeScopeKind.Tree, "legacy-tree")],
            });
        var okOutcome = await ok.RunAsync(AppActivationOperation.Enable);
        Assert.That(okOutcome.Failure, Is.Not.EqualTo(AppActivationFailure.CeilingExceeded));
    }

    // ----- a manifest carrying a null tree -----

    [Test]
    public void The_ownership_plan_skips_a_null_tree_declaration()
    {
        // Manifests arrive deserialized from JSON, where a null array element is
        // representable. A null entry must be skipped rather than dereferenced: an
        // exception here would abort the whole claim plan and block an install that
        // is otherwise valid.
        var manifest = new AppManifest
        {
            Identity = new AppIdentity { Slug = ActivationHarness.Slug, Version = ActivationHarness.V1 },
            Trees = [null!, ActivationHarness.Tree("records")],
            Roles = [],
            Subscriptions = [],
            McpTools = [],
        };

        var plan = AppTreeOwnershipLedger.Plan(manifest, TenantId.Default);

        Assert.That(plan.Select(p => p.TreeName), Is.EqualTo(new[] { "records" }),
            "the null entry must be skipped and the real declaration still planned");
    }

    // ----- a duplicate role binding -----

    [Test]
    public void Binding_the_same_role_twice_is_refused()
    {
        // A role binds to exactly one group. Two bindings for one role would compile
        // into two grants whose relative precedence is undefined, so the request is
        // refused at the front door rather than resolved arbitrarily.
        var registry = AppRegistryTestData.CreateRegistry(new InMemoryAppRegistryStore());

        Assert.That(
            async () => await registry.InstallAsync(AppRegistryTestData.Request(bindings: new[]
            {
                AppRoleBinding.Create("reader", "readers"),
                AppRoleBinding.Create("reader", "auditors"),
            })),
            Throws.ArgumentException.With.Message.Contains("bound more than once"));
    }

    [Test]
    public async Task Binding_two_different_roles_is_accepted()
    {
        // The accepting counterpart: the duplicate check must key on the role name,
        // not merely reject any request carrying more than one binding.
        var registry = AppRegistryTestData.CreateRegistry(new InMemoryAppRegistryStore());

        var result = await registry.InstallAsync(AppRegistryTestData.Request(bindings: new[]
        {
            AppRoleBinding.Create("reader", "readers"),
            AppRoleBinding.Create("writer", "writers"),
        }));

        Assert.That(result.Succeeded, Is.True, result.Message);
    }

    // ----- soft-deleting a tree that is not registered -----

    [Test]
    public async Task Soft_deleting_an_unregistered_tree_is_a_no_op()
    {
        // Retirement runs against whatever the applied manifest recorded, which can
        // name a tree that was never provisioned (an interrupted activation, or a
        // tree already reclaimed). Dialing the tree grain regardless would activate
        // a grain for a tree that does not exist, creating the very state the delete
        // was meant to remove.
        var factory = Substitute.For<IGrainFactory>();
        var registry = Substitute.For<ILatticeRegistry>();
        registry.ExistsAsync("a/notes/gone").Returns(false);
        factory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).Returns(registry);
        var tree = Substitute.For<ILattice>();
        factory.GetGrain<ILattice>("a/notes/gone").Returns(tree);

        await new LatticeAppTreeProvisioner(factory).SoftDeleteAsync("a/notes/gone", CancellationToken.None);

        await tree.DidNotReceiveWithAnyArgs().DeleteTreeAsync(default);
    }

    [Test]
    public async Task Soft_deleting_a_registered_tree_deletes_it()
    {
        // The accepting counterpart over the same wiring, so the no-op above is the
        // existence check and not a provisioner that never deletes anything.
        var factory = Substitute.For<IGrainFactory>();
        var registry = Substitute.For<ILatticeRegistry>();
        registry.ExistsAsync("a/notes/records").Returns(true);
        factory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).Returns(registry);
        var tree = Substitute.For<ILattice>();
        factory.GetGrain<ILattice>("a/notes/records").Returns(tree);

        await new LatticeAppTreeProvisioner(factory).SoftDeleteAsync("a/notes/records", CancellationToken.None);

        await tree.Received(1).DeleteTreeAsync(Arg.Any<CancellationToken>());
    }
}

using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using Orleans.Lattice.Api.Schema;
using Orleans.Lattice.Explorer.Core.Connection;
using Orleans.Lattice.Explorer.UI.Areas.Schema;
using Orleans.Lattice.Explorer.UI.Design.Components;
using Orleans.Lattice.Explorer.UI.Transport;
using Orleans.Lattice.Schema;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Schema;

[TestFixture]
public sealed class SchemaMemberSuggestionSourceTests
{
    [Test]
    public async Task SuggestAsync_cancelled_policy_read_propagates_and_does_not_poison_the_next_query()
    {
        var schema = Substitute.For<ILatticeSchemaControl>();
        using var services = Services(schema).BuildServiceProvider();
        var source = new SchemaMemberSuggestionSource(new SchemaFacades(services), () => "orders");
        using var request = new CancellationTokenSource();
        var reply = new TaskCompletionSource<LatticeSchemaPolicy?>(TaskCreationOptions.RunContinuationsAsynchronously);
        schema.GetPolicyAsync("orders", request.Token).Returns(reply.Task);

        var running = source.SuggestAsync("", 5, request.Token).AsTask();
        Assert.That(running.IsCompleted, Is.False);
        request.Cancel();
        reply.SetCanceled(request.Token);
        var thrown = Assert.ThrowsAsync<TaskCanceledException>(
            async () => await running.WaitAsync(TimeSpan.FromSeconds(5)));
        Assert.That(thrown!.CancellationToken, Is.EqualTo(request.Token));

        schema.GetPolicyAsync("orders", CancellationToken.None).Returns(Task.FromResult<LatticeSchemaPolicy?>(Policy("customer")));
        var recovered = await source.SuggestAsync("", 5, CancellationToken.None);

        Assert.That(recovered.Items.Select(item => item.Value), Is.EqualTo(new[] { "customer" }));
        await schema.Received(1).GetPolicyAsync("orders", request.Token);
        await schema.Received(1).GetPolicyAsync("orders", CancellationToken.None);
    }

    [TestCase(false)]
    [TestCase(true)]
    public async Task SuggestAsync_faults_are_unavailable_until_invalidated_not_empty_or_private_details(bool unrelatedCancellation)
    {
        var schema = Substitute.For<ILatticeSchemaControl>();
        using var services = Services(schema).BuildServiceProvider();
        var source = new SchemaMemberSuggestionSource(new SchemaFacades(services), () => "orders");
        Exception fault = unrelatedCancellation
            ? new OperationCanceledException("private failure")
            : new InvalidOperationException("private failure");
        schema.GetPolicyAsync("orders", CancellationToken.None)
            .Returns(Task.FromException<LatticeSchemaPolicy?>(fault));

        var failed = await source.SuggestAsync("", 5, CancellationToken.None);
        schema.GetPolicyAsync("orders", CancellationToken.None)
            .Returns(Task.FromResult<LatticeSchemaPolicy?>(Policy("customer")));
        var remembered = await source.SuggestAsync("", 5, CancellationToken.None);
        await schema.Received(1).GetPolicyAsync("orders", CancellationToken.None);
        source.Invalidate();
        var recovered = await source.SuggestAsync("", 5, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(failed.IsAvailable, Is.False);
            Assert.That(failed.Items, Is.Empty);
            Assert.That(failed.UnavailableReason, Is.EqualTo(SchemaMemberSuggestionSource.UnavailableReason));
            Assert.That(remembered.IsAvailable, Is.False);
            Assert.That(recovered.IsAvailable, Is.True);
            Assert.That(recovered.Items.Select(item => item.Value), Is.EqualTo(new[] { "customer" }));
        });
        await schema.Received(2).GetPolicyAsync("orders", CancellationToken.None);
    }

    [Test]
    public async Task SuggestAsync_an_answer_overtaken_by_a_tenant_change_is_not_remembered()
    {
        var schema = Substitute.For<ILatticeSchemaControl>();
        var tenant = Substitute.For<ILatticeActiveTenantProvider>();
        tenant.AssertedTenant.Returns("acme");
        using var services = Services(schema).AddSingleton(tenant).BuildServiceProvider();
        var source = new SchemaMemberSuggestionSource(new SchemaFacades(services), () => "orders");
        var reply = new TaskCompletionSource<LatticeSchemaPolicy?>(TaskCreationOptions.RunContinuationsAsynchronously);
        schema.GetPolicyAsync("orders", CancellationToken.None).Returns(reply.Task);

        var old = source.SuggestAsync("", 5, CancellationToken.None).AsTask();
        Assert.That(old.IsCompleted, Is.False);
        tenant.AssertedTenant.Returns("other");
        reply.SetResult(Policy("old.member"));
        var previousAnswer = await old.WaitAsync(TimeSpan.FromSeconds(5));
        schema.GetPolicyAsync("orders", CancellationToken.None)
            .Returns(Task.FromResult<LatticeSchemaPolicy?>(Policy("new.member")));
        var current = await source.SuggestAsync("", 5, CancellationToken.None);
        var cached = await source.SuggestAsync("", 5, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(previousAnswer.Items.Single().Value, Is.EqualTo("old.member"));
            Assert.That(current.Items.Single().Value, Is.EqualTo("new.member"));
            Assert.That(cached.Items.Single().Value, Is.EqualTo("new.member"));
        });
        await schema.Received(2).GetPolicyAsync("orders", CancellationToken.None);
    }

    [Test]
    public async Task SuggestAsync_members_are_unique_sorted_filtered_and_refreshed_when_the_tree_changes()
    {
        var schema = Substitute.For<ILatticeSchemaControl>();
        using var services = Services(schema).BuildServiceProvider();
        string? tree = "orders";
        var source = new SchemaMemberSuggestionSource(new SchemaFacades(services), () => tree);
        schema.GetPolicyAsync("orders", CancellationToken.None).Returns(Task.FromResult<LatticeSchemaPolicy?>(
            new LatticeSchemaPolicy([
                LatticeSchemaRule.Regex(".", "z.last"),
                LatticeSchemaRule.Regex(".", "a.first"),
                LatticeSchemaRule.Regex("..", "a.first"),
                LatticeSchemaRule.Json(),
            ])));
        schema.GetPolicyAsync("audit", CancellationToken.None).Returns(Task.FromResult<LatticeSchemaPolicy?>(null));

        var all = await source.SuggestAsync("", 5, CancellationToken.None);
        var limited = await source.SuggestAsync("", 1, CancellationToken.None);
        var filtered = await source.SuggestAsync("z.", 5, CancellationToken.None);
        tree = "audit";
        var absent = await source.SuggestAsync("", 5, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(all.Items.Select(item => item.Value), Is.EqualTo(new[] { "a.first", "z.last" }));
            Assert.That(limited.Items, Has.Count.EqualTo(1));
            Assert.That(filtered.Items.Single().Value, Is.EqualTo("z.last"));
            Assert.That(absent.IsAvailable, Is.True);
            Assert.That(absent.Items, Is.Empty);
        });
        await schema.Received(1).GetPolicyAsync("orders", CancellationToken.None);
        await schema.Received(1).GetPolicyAsync("audit", CancellationToken.None);
    }

    [TestCase(null)]
    [TestCase("")]
    public async Task SuggestAsync_without_a_tree_makes_no_policy_read(string? tree)
    {
        var schema = Substitute.For<ILatticeSchemaControl>();
        using var services = Services(schema).BuildServiceProvider();
        var source = new SchemaMemberSuggestionSource(new SchemaFacades(services), () => tree);

        Assert.That(await source.SuggestAsync("", 5, CancellationToken.None), Is.SameAs(LtSuggestionSet.Empty));
        Assert.That(schema.ReceivedCalls(), Is.Empty);
        var thrown = Assert.ThrowsAsync<ArgumentNullException>(
            async () => await source.SuggestAsync(null!, 5, CancellationToken.None));
        Assert.That(thrown!.ParamName, Is.EqualTo("text"));
    }

    private static ServiceCollection Services(ILatticeSchemaControl schema)
    {
        var services = new ServiceCollection();
        services.AddKeyedSingleton(ShellFacades.Key, schema);
        return services;
    }

    private static LatticeSchemaPolicy Policy(string member) => new([LatticeSchemaRule.Regex(".", member)]);
}

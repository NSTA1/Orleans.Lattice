using Orleans.Lattice.Api.Operations;

namespace Orleans.Lattice.Api.Abstractions.Tests;

/// <summary>
/// Unit tests for the shared long-running-operation contract (#4122):
/// <see cref="LatticeOperationStatus.IsTerminal"/> per state, the list request's
/// defaulted and clamped page size, and the defaults of the page, scope and status.
/// </summary>
[TestFixture]
public sealed class OperationModelsTests
{
    private static LatticeOperationStatus Status(LatticeOperationState state) => new()
    {
        OperationId = "op",
        Kind = "backup.capture",
        Scope = new LatticeOperationScope { TenantId = "default" },
        State = state,
        Phase = "Capturing",
    };

    [TestCase(LatticeOperationState.Queued, false)]
    [TestCase(LatticeOperationState.Running, false)]
    [TestCase(LatticeOperationState.Succeeded, true)]
    [TestCase(LatticeOperationState.Failed, true)]
    [TestCase(LatticeOperationState.Cancelled, true)]
    public void IsTerminal_is_true_only_for_final_states(LatticeOperationState state, bool terminal)
    {
        Assert.That(Status(state).IsTerminal, Is.EqualTo(terminal));
    }

    [TestCase(0, LatticeOperationListRequest.DefaultPageSize)]
    [TestCase(-5, LatticeOperationListRequest.DefaultPageSize)]
    [TestCase(7, 7)]
    [TestCase(10_000, LatticeOperationListRequest.MaxPageSize)]
    public void EffectivePageSize_defaults_and_clamps(int requested, int effective)
    {
        Assert.That(new LatticeOperationListRequest { PageSize = requested }.EffectivePageSize, Is.EqualTo(effective));
    }

    [Test]
    public void Defaults_are_empty_rather_than_null()
    {
        var status = Status(LatticeOperationState.Running);

        Assert.Multiple(() =>
        {
            Assert.That(new LatticeOperationPage().Operations, Is.Empty);
            Assert.That(new LatticeOperationPage().NextPageToken, Is.Null);
            Assert.That(status.Scope.TreeIds, Is.Empty);
            Assert.That(status.Result, Is.Empty);
            Assert.That(status.TotalUnits, Is.Null, "An unknown total is null, never a fabricated number.");
            Assert.That(new LatticeOperationListRequest().PageToken, Is.Null);
        });
    }

    [Test]
    public void The_handle_carries_id_kind_scope_and_whether_it_was_created()
    {
        var handle = new LatticeOperationHandle
        {
            OperationId = "op",
            Kind = "backup.restore",
            Scope = new LatticeOperationScope { TenantId = "acme", TreeIds = ["t"] },
            Created = true,
        };

        Assert.Multiple(() =>
        {
            Assert.That(handle.Scope.TenantId, Is.EqualTo("acme"));
            Assert.That(handle.Scope.TreeIds, Is.EqualTo(new[] { "t" }));
            Assert.That(handle.Created, Is.True);
        });
    }

    [Test]
    public void Every_alias_uses_the_reserved_prefix_and_is_unique()
    {
        var aliases = typeof(ApiOperationTypeAliases)
            .GetFields()
            .Where(f => f.IsLiteral && f.Name != nameof(ApiOperationTypeAliases.AliasPrefix))
            .Select(f => (string)f.GetRawConstantValue()!)
            .ToList();

        Assert.Multiple(() =>
        {
            Assert.That(aliases, Is.Not.Empty);
            Assert.That(aliases, Is.Unique);
            Assert.That(aliases, Has.All.StartsWith(ApiOperationTypeAliases.AliasPrefix));
            Assert.That(aliases, Has.All.Length.LessThanOrEqualTo(6));
        });
    }
}

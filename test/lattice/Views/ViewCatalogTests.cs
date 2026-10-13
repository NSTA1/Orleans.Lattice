using NUnit.Framework;
using Orleans.Lattice.Views;

namespace Orleans.Lattice.Tests.Views;

/// <summary>
/// Unit tests for <see cref="ViewCatalog"/>'s compare-and-swap and durability
/// marks. <see cref="ViewRegistration"/> is a record, so a newer registration can
/// be equal by value to a stale one; <see cref="ViewCatalog.TryReplace"/> must
/// still compare by reference, and replacing or removing an entry must discard
/// its durability mark rather than leak it.
/// </summary>
[TestFixture]
public sealed class ViewCatalogTests
{
    private static readonly ILatticeViewProjection Projection = new NoOpProjection();

    [Test]
    public void TryReplace_does_not_replace_a_newer_registration_equal_by_value_to_the_expected_one()
    {
        var catalog = new ViewCatalog();
        var stale = Registration("v", "src");
        var current = stale with { };
        Assert.That(current, Is.EqualTo(stale).And.Not.SameAs(stale), "Precondition: equal by value, distinct by reference.");
        catalog.Register(stale);
        catalog.Register(current);

        var replaced = catalog.TryReplace(stale, Registration("v", "other"));

        Assert.Multiple(() =>
        {
            Assert.That(replaced, Is.False);
            Assert.That(catalog.TryGet("v"), Is.SameAs(current));
        });
    }

    [Test]
    public void TryReplace_with_null_does_not_remove_a_newer_registration_equal_by_value_to_the_expected_one()
    {
        var catalog = new ViewCatalog();
        var stale = Registration("v", "src");
        var current = stale with { };
        catalog.Register(stale);
        catalog.Register(current);

        var removed = catalog.TryReplace(stale, replacement: null);

        Assert.Multiple(() =>
        {
            Assert.That(removed, Is.False);
            Assert.That(catalog.TryGet("v"), Is.SameAs(current));
        });
    }

    [Test]
    public void TryReplace_replaces_or_removes_the_bound_registration()
    {
        var catalog = new ViewCatalog();
        var bound = Registration("v", "src");
        var replacement = Registration("v", "other");
        catalog.Register(bound);

        Assert.That(catalog.TryReplace(bound, replacement), Is.True);
        Assert.That(catalog.TryGet("v"), Is.SameAs(replacement));
        Assert.That(catalog.TryReplace(replacement, replacement: null), Is.True);
        Assert.That(catalog.TryGet("v"), Is.Null);
    }

    [Test]
    public void Register_replacing_an_entry_discards_the_replaced_entrys_mark_but_keeps_its_own()
    {
        var catalog = new ViewCatalog();
        var first = Registration("v", "src");
        var second = first with { };
        catalog.MarkDurablyPending(first);
        catalog.Register(first);
        catalog.MarkDurablyPending(second);
        catalog.Register(second);

        Assert.Multiple(() =>
        {
            Assert.That(catalog.GetDurability(first), Is.EqualTo(ViewCatalogDurability.Settled));
            Assert.That(catalog.GetDurability(second), Is.EqualTo(ViewCatalogDurability.PersistInFlight));
        });
    }

    [Test]
    public void Re_registering_the_same_instance_keeps_its_mark()
    {
        var catalog = new ViewCatalog();
        var registration = Registration("v", "src");
        catalog.MarkDurablyPending(registration);
        catalog.Register(registration);
        catalog.Register(registration);

        Assert.That(catalog.GetDurability(registration), Is.EqualTo(ViewCatalogDurability.PersistInFlight));
    }

    [Test]
    public void TryReplace_and_Remove_discard_the_retired_entrys_mark()
    {
        var catalog = new ViewCatalog();
        var replaced = Registration("v", "src");
        var replacement = Registration("v", "other");
        catalog.MarkDurablyPending(replaced);
        catalog.Register(replaced);
        Assert.That(catalog.TryReplace(replaced, replacement), Is.True);

        catalog.MarkDurablyPending(replacement);
        catalog.Remove("v");

        Assert.Multiple(() =>
        {
            Assert.That(catalog.GetDurability(replaced), Is.EqualTo(ViewCatalogDurability.Settled));
            Assert.That(catalog.GetDurability(replacement), Is.EqualTo(ViewCatalogDurability.Settled));
        });
    }

    [Test]
    public void A_failed_persist_moves_a_bound_in_flight_entry_to_failed_and_success_clears_it()
    {
        var catalog = new ViewCatalog();
        var failed = Registration("a", "src");
        catalog.MarkDurablyPending(failed);
        catalog.Register(failed);
        catalog.MarkDurablePersistFailed(failed);

        var persisted = Registration("b", "src");
        catalog.MarkDurablyPending(persisted);
        catalog.Register(persisted);
        catalog.ClearDurablyPending(persisted);

        Assert.Multiple(() =>
        {
            Assert.That(catalog.GetDurability(failed), Is.EqualTo(ViewCatalogDurability.PersistFailed));
            Assert.That(catalog.GetDurability(persisted), Is.EqualTo(ViewCatalogDurability.Settled));
        });
    }

    [Test]
    public void A_failed_persist_does_not_mark_an_entry_already_replaced_or_unmarked()
    {
        var catalog = new ViewCatalog();
        var superseded = Registration("v", "src");
        catalog.MarkDurablyPending(superseded);
        catalog.Register(superseded);
        catalog.Register(Registration("v", "other"));
        catalog.MarkDurablePersistFailed(superseded);

        var unmarked = Registration("w", "src");
        catalog.Register(unmarked);
        catalog.MarkDurablePersistFailed(unmarked);

        Assert.Multiple(() =>
        {
            Assert.That(catalog.GetDurability(superseded), Is.EqualTo(ViewCatalogDurability.Settled));
            Assert.That(catalog.GetDurability(unmarked), Is.EqualTo(ViewCatalogDurability.Settled));
        });
    }

    [Test]
    public void Concurrent_compare_and_swaps_from_one_expected_entry_admit_exactly_one_winner()
    {
        var catalog = new ViewCatalog();
        var expected = Registration("v", "src");
        catalog.Register(expected);
        var candidates = Enumerable.Range(0, 32).Select(_ => expected with { }).ToArray();

        var wins = 0;
        Parallel.ForEach(candidates, candidate =>
        {
            if (catalog.TryReplace(expected, candidate))
            {
                Interlocked.Increment(ref wins);
            }
        });

        Assert.Multiple(() =>
        {
            Assert.That(wins, Is.EqualTo(1));
            Assert.That(candidates, Has.Some.SameAs(catalog.TryGet("v")));
        });
    }

    private static ViewRegistration Registration(string viewName, string sourceTreeId) =>
        new(viewName, sourceTreeId, Projection, ProjectionProviderKey: "provider");

    private sealed class NoOpProjection : ILatticeViewProjection
    {
        public string ProjectionVersion => "v1";

        public IEnumerable<ViewWrite> Project(LatticeMutation mutation) => [];
    }
}
using Orleans.Lattice.Explorer.DesignSystem.Layout;

namespace Orleans.Lattice.Explorer.Tests.DesignSystem;

/// <summary>
/// Unit tests for the deterministic inline-versus-overflow split that both the
/// adaptive tab strip and the compact navigation bar are built on. The rule
/// under test is that the active item is always inline, which is what stops a
/// collapsed strip hiding where the caller is.
/// </summary>
[TestFixture]
public sealed class LatticeOverflowLayoutTests
{
    [Test]
    public void Empty_is_the_default_value()
    {
        Assert.That(LatticeOverflowLayout.Empty, Is.EqualTo(default(LatticeOverflowLayout)));
    }

    [Test]
    public void Empty_reports_no_items_and_no_overflow()
    {
        var layout = LatticeOverflowLayout.Empty;

        Assert.Multiple(() =>
        {
            Assert.That(layout.TotalCount, Is.Zero);
            Assert.That(layout.InlineCount, Is.Zero);
            Assert.That(layout.HasOverflow, Is.False);
            Assert.That(layout.PromotesActive, Is.False);
            Assert.That(layout.IsInline(0), Is.False);
            Assert.That(layout.IsOverflowed(0), Is.False);
        });
    }

    [TestCase(0)]
    [TestCase(-1)]
    [TestCase(int.MinValue)]
    public void Resolve_treats_a_non_positive_count_as_empty(int totalCount)
    {
        Assert.That(LatticeOverflowLayout.Resolve(totalCount, 0, 4), Is.EqualTo(LatticeOverflowLayout.Empty));
    }

    [Test]
    public void Resolve_keeps_every_item_inline_when_the_strip_fits()
    {
        var layout = LatticeOverflowLayout.Resolve(totalCount: 3, activeIndex: 0, inlineCapacity: 4);

        Assert.Multiple(() =>
        {
            Assert.That(layout.HasOverflow, Is.False);
            Assert.That(layout.InlineCount, Is.EqualTo(3));
            Assert.That(layout.PromotesActive, Is.False);
            Assert.That(layout.IsInline(0), Is.True);
            Assert.That(layout.IsInline(1), Is.True);
            Assert.That(layout.IsInline(2), Is.True);
        });
    }

    [Test]
    public void Resolve_clamps_the_capacity_down_to_the_item_count()
    {
        var layout = LatticeOverflowLayout.Resolve(totalCount: 2, activeIndex: 0, inlineCapacity: 99);

        Assert.Multiple(() =>
        {
            Assert.That(layout.InlineCapacity, Is.EqualTo(2));
            Assert.That(layout.HasOverflow, Is.False);
        });
    }

    [TestCase(0)]
    [TestCase(-5)]
    public void Resolve_clamps_a_non_positive_capacity_up_to_one(int capacity)
    {
        var layout = LatticeOverflowLayout.Resolve(totalCount: 5, activeIndex: 0, inlineCapacity: capacity);

        Assert.Multiple(() =>
        {
            Assert.That(layout.InlineCapacity, Is.EqualTo(1));
            Assert.That(layout.InlineCount, Is.EqualTo(1));
            Assert.That(layout.HasOverflow, Is.True);
        });
    }

    [Test]
    public void Resolve_overflows_the_tail_when_the_active_item_already_fits()
    {
        var layout = LatticeOverflowLayout.Resolve(totalCount: 6, activeIndex: 1, inlineCapacity: 4);

        Assert.Multiple(() =>
        {
            Assert.That(layout.HasOverflow, Is.True);
            Assert.That(layout.PromotesActive, Is.False);
            Assert.That(layout.InlineCount, Is.EqualTo(4));
            Assert.That(layout.IsInline(0), Is.True);
            Assert.That(layout.IsInline(1), Is.True);
            Assert.That(layout.IsInline(2), Is.True);
            Assert.That(layout.IsInline(3), Is.True);
            Assert.That(layout.IsOverflowed(4), Is.True);
            Assert.That(layout.IsOverflowed(5), Is.True);
        });
    }

    [Test]
    public void Resolve_promotes_the_active_item_into_the_last_inline_slot()
    {
        var layout = LatticeOverflowLayout.Resolve(totalCount: 6, activeIndex: 5, inlineCapacity: 4);

        Assert.Multiple(() =>
        {
            Assert.That(layout.PromotesActive, Is.True);
            Assert.That(layout.InlineCount, Is.EqualTo(4));
            Assert.That(layout.IsInline(0), Is.True);
            Assert.That(layout.IsInline(1), Is.True);
            Assert.That(layout.IsInline(2), Is.True);

            // Index 3 lost its slot to the promoted active item.
            Assert.That(layout.IsOverflowed(3), Is.True);
            Assert.That(layout.IsOverflowed(4), Is.True);
            Assert.That(layout.IsInline(5), Is.True);
        });
    }

    [Test]
    public void Resolve_keeps_the_inline_count_equal_to_the_capacity_when_promoting()
    {
        var layout = LatticeOverflowLayout.Resolve(totalCount: 10, activeIndex: 9, inlineCapacity: 4);

        var inline = 0;
        for (var i = 0; i < layout.TotalCount; i++)
        {
            if (layout.IsInline(i))
            {
                inline++;
            }
        }

        Assert.That(inline, Is.EqualTo(layout.InlineCount));
    }

    [Test]
    public void Resolve_shows_only_the_active_item_when_the_capacity_is_one()
    {
        var layout = LatticeOverflowLayout.Resolve(totalCount: 5, activeIndex: 3, inlineCapacity: 1);

        Assert.Multiple(() =>
        {
            Assert.That(layout.PromotesActive, Is.True);
            Assert.That(layout.InlineCount, Is.EqualTo(1));
            Assert.That(layout.IsInline(3), Is.True);
            Assert.That(layout.IsInline(0), Is.False);
            Assert.That(layout.IsInline(4), Is.False);
        });
    }

    [TestCase(-1)]
    [TestCase(7)]
    [TestCase(int.MaxValue)]
    public void Resolve_treats_an_out_of_range_active_index_as_nothing_active(int activeIndex)
    {
        var layout = LatticeOverflowLayout.Resolve(totalCount: 5, activeIndex: activeIndex, inlineCapacity: 2);

        Assert.Multiple(() =>
        {
            Assert.That(layout.ActiveIndex, Is.EqualTo(-1));
            Assert.That(layout.PromotesActive, Is.False);
            Assert.That(layout.IsInline(0), Is.True);
            Assert.That(layout.IsInline(1), Is.True);
            Assert.That(layout.IsOverflowed(2), Is.True);
        });
    }

    [Test]
    public void The_active_item_is_inline_for_every_capacity_and_position()
    {
        // The load-bearing invariant: whatever the strip length, the capacity, or
        // where the active item sits, it is never hidden behind the overflow.
        for (var total = 1; total <= 12; total++)
        {
            for (var capacity = 1; capacity <= 12; capacity++)
            {
                for (var active = 0; active < total; active++)
                {
                    var layout = LatticeOverflowLayout.Resolve(total, active, capacity);
                    Assert.That(
                        layout.IsInline(active),
                        Is.True,
                        $"active index {active} of {total} at capacity {capacity} must stay inline");
                }
            }
        }
    }

    [Test]
    public void Every_index_is_either_inline_or_overflowed_but_never_both()
    {
        for (var total = 1; total <= 12; total++)
        {
            for (var capacity = 1; capacity <= 12; capacity++)
            {
                for (var active = -1; active < total; active++)
                {
                    var layout = LatticeOverflowLayout.Resolve(total, active, capacity);
                    for (var i = 0; i < total; i++)
                    {
                        Assert.That(
                            layout.IsInline(i) ^ layout.IsOverflowed(i),
                            Is.True,
                            $"index {i} of {total} at capacity {capacity}, active {active}");
                    }
                }
            }
        }
    }

    [Test]
    public void The_inline_count_always_matches_the_number_of_inline_indices()
    {
        for (var total = 1; total <= 12; total++)
        {
            for (var capacity = 1; capacity <= 12; capacity++)
            {
                for (var active = -1; active < total; active++)
                {
                    var layout = LatticeOverflowLayout.Resolve(total, active, capacity);

                    var inline = 0;
                    for (var i = 0; i < total; i++)
                    {
                        if (layout.IsInline(i))
                        {
                            inline++;
                        }
                    }

                    Assert.That(
                        inline,
                        Is.EqualTo(layout.InlineCount),
                        $"{total} items at capacity {capacity}, active {active}");
                }
            }
        }
    }

    [Test]
    public void IsInline_and_IsOverflowed_reject_an_index_outside_the_strip()
    {
        var layout = LatticeOverflowLayout.Resolve(totalCount: 3, activeIndex: 0, inlineCapacity: 2);

        Assert.Multiple(() =>
        {
            Assert.That(layout.IsInline(-1), Is.False);
            Assert.That(layout.IsInline(3), Is.False);
            Assert.That(layout.IsOverflowed(-1), Is.False);
            Assert.That(layout.IsOverflowed(3), Is.False);
        });
    }

    [Test]
    public void Two_layouts_resolved_from_the_same_inputs_are_equal()
    {
        var first = LatticeOverflowLayout.Resolve(8, 6, 3);
        var second = LatticeOverflowLayout.Resolve(8, 6, 3);

        Assert.Multiple(() =>
        {
            Assert.That(first, Is.EqualTo(second));
            Assert.That(first.GetHashCode(), Is.EqualTo(second.GetHashCode()));
            Assert.That(first.ToString(), Is.EqualTo(second.ToString()));
        });
    }

    [Test]
    public void Layouts_resolved_from_different_inputs_are_not_equal()
    {
        Assert.That(
            LatticeOverflowLayout.Resolve(8, 6, 3),
            Is.Not.EqualTo(LatticeOverflowLayout.Resolve(8, 1, 3)));
    }
}

using Orleans.Runtime;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Unit tests for <see cref="LatticeApplyOffsetContext"/>: the ambient WAL
/// (partition, offset) hint the replay coordinator stamps around each
/// <c>ILeafProjection.Apply</c> so the leaf can clamp its projection checkpoint back
/// behind any unresolved saga prepare.
/// <para>
/// The correctness that matters here is scope discipline: the foreground commit path
/// must observe no hint at all (it is the WAL author, not its replayer), and a nested
/// scope must restore its predecessor exactly - including restoring "absent" rather
/// than leaking a stale offset - because a hint that outlives its apply would clamp an
/// unrelated checkpoint and could advance it past an unresolved prepare.
/// </para>
/// </summary>
[TestFixture]
[Category("Unit")]
public sealed class LatticeApplyOffsetContextTests
{
    [SetUp]
    [TearDown]
    public void ClearAmbientContext() => RequestContext.Clear();

    [Test]
    public void Current_is_null_outside_any_scope()
    {
        Assert.Multiple(() =>
        {
            Assert.That(LatticeApplyOffsetContext.Current, Is.Null);
            Assert.That(LatticeApplyOffsetContext.CurrentPartition, Is.Null);
        });
    }

    [Test]
    public void BeginScope_offset_only_defaults_to_partition_zero()
    {
        using (LatticeApplyOffsetContext.BeginScope(42L))
        {
            Assert.Multiple(() =>
            {
                Assert.That(LatticeApplyOffsetContext.Current, Is.EqualTo(42L));
                Assert.That(LatticeApplyOffsetContext.CurrentPartition, Is.EqualTo(0));
            });
        }
    }

    [Test]
    public void BeginScope_stamps_the_partition_and_offset_pair()
    {
        using (LatticeApplyOffsetContext.BeginScope(3, 99L))
        {
            Assert.Multiple(() =>
            {
                Assert.That(LatticeApplyOffsetContext.Current, Is.EqualTo(99L));
                Assert.That(LatticeApplyOffsetContext.CurrentPartition, Is.EqualTo(3));
            });
        }
    }

    [Test]
    public void BeginScope_rejects_a_negative_partition()
    {
        var ex = Assert.Throws<ArgumentOutOfRangeException>(
            () => LatticeApplyOffsetContext.BeginScope(-1, 5L));
        Assert.That(ex!.ParamName, Is.EqualTo("partition"));

        // A rejected scope must not have stamped anything.
        Assert.That(LatticeApplyOffsetContext.Current, Is.Null);
    }

    [Test]
    public void Disposing_a_scope_removes_the_hint_when_there_was_no_prior_value()
    {
        using (LatticeApplyOffsetContext.BeginScope(1, 7L))
        {
            Assert.That(LatticeApplyOffsetContext.Current, Is.EqualTo(7L));
        }

        // Removed rather than reset to a default: the foreground path must see "absent".
        Assert.Multiple(() =>
        {
            Assert.That(LatticeApplyOffsetContext.Current, Is.Null);
            Assert.That(LatticeApplyOffsetContext.CurrentPartition, Is.Null);
        });
    }

    [Test]
    public void Disposing_a_nested_scope_restores_the_enclosing_values()
    {
        using (LatticeApplyOffsetContext.BeginScope(1, 10L))
        {
            using (LatticeApplyOffsetContext.BeginScope(2, 20L))
            {
                Assert.Multiple(() =>
                {
                    Assert.That(LatticeApplyOffsetContext.Current, Is.EqualTo(20L));
                    Assert.That(LatticeApplyOffsetContext.CurrentPartition, Is.EqualTo(2));
                });
            }

            Assert.Multiple(() =>
            {
                Assert.That(LatticeApplyOffsetContext.Current, Is.EqualTo(10L));
                Assert.That(LatticeApplyOffsetContext.CurrentPartition, Is.EqualTo(1));
            });
        }

        Assert.That(LatticeApplyOffsetContext.Current, Is.Null);
    }

    [Test]
    public void Disposing_a_scope_twice_is_idempotent()
    {
        using (LatticeApplyOffsetContext.BeginScope(1, 10L))
        {
            var inner = LatticeApplyOffsetContext.BeginScope(2, 20L);
            inner.Dispose();
            Assert.That(LatticeApplyOffsetContext.Current, Is.EqualTo(10L));

            // A second dispose must not re-run the restore and clobber the enclosing
            // scope's values.
            inner.Dispose();
            Assert.Multiple(() =>
            {
                Assert.That(LatticeApplyOffsetContext.Current, Is.EqualTo(10L));
                Assert.That(LatticeApplyOffsetContext.CurrentPartition, Is.EqualTo(1));
            });
        }
    }

    [Test]
    public void Current_ignores_a_foreign_value_on_the_request_context()
    {
        RequestContext.Set(LatticeEventConstants.ApplyOffsetRequestContextKey, "not-a-long");
        RequestContext.Set(LatticeEventConstants.ApplyOffsetPartitionRequestContextKey, "not-an-int");

        Assert.Multiple(() =>
        {
            Assert.That(LatticeApplyOffsetContext.Current, Is.Null);
            Assert.That(LatticeApplyOffsetContext.CurrentPartition, Is.Null);
        });
    }
}

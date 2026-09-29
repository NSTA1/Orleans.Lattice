namespace Orleans.Lattice.Tenancy.Tests;

/// <summary>
/// Unit tests for the T20 per-tenant region-residency extensions on
/// <see cref="TenantRecord"/>: the operator-written allowed set
/// (<c>AuthorizeRegion</c> / <c>RevokeRegion</c> / <c>IsRegionAllowed</c> /
/// <c>AllowedRegionIds</c>) and the tenant-admin / lifecycle-written status map
/// (<c>SetRegionStatus</c> / <c>GetRegionStatus</c> / <c>HasResidencyConfiguration</c>
/// / <c>ResidentRegionCount</c> / <c>RegionStatusEntries</c>), plus their CRDT
/// convergence under <see cref="TenantRecord.Merge"/>. Every stamp is built by
/// hand so convergence is a deterministic property of the stamp order alone.
/// </summary>
[TestFixture]
public sealed class TenantRecordRegionResidencyTests
{
    private static TenantRecord NewRecord(string tenant = "acme") =>
        TenantRecord.Create(
            TenantId.Parse(tenant),
            TenantStatus.Active,
            TenantQuotas.Unbounded,
            TenantPlacement.Shared,
            TestClocks.Clock(1),
            writerId: "op");

    [Test]
    public void AuthorizeRegion_makes_the_region_allowed()
    {
        var record = NewRecord();

        record.AuthorizeRegion("region-a", TestClocks.Clock(2), "op");

        Assert.That(record.IsRegionAllowed("region-a"), Is.True);
    }

    [Test]
    public void RevokeRegion_with_a_higher_stamp_removes_the_region()
    {
        var record = NewRecord();
        record.AuthorizeRegion("region-a", TestClocks.Clock(2), "op");

        record.RevokeRegion("region-a", TestClocks.Clock(3), "op");

        Assert.That(record.IsRegionAllowed("region-a"), Is.False);
    }

    [Test]
    public void RevokeRegion_with_an_older_stamp_is_ignored()
    {
        var record = NewRecord();
        record.AuthorizeRegion("region-a", TestClocks.Clock(5), "op");

        record.RevokeRegion("region-a", TestClocks.Clock(2), "op");

        Assert.That(record.IsRegionAllowed("region-a"), Is.True);
    }

    [Test]
    public void IsRegionAllowed_is_false_for_an_unknown_region()
    {
        var record = NewRecord();

        Assert.That(record.IsRegionAllowed("region-x"), Is.False);
    }

    [Test]
    public void AllowedRegionIds_returns_only_present_regions_in_ordinal_order()
    {
        var record = NewRecord();
        record.AuthorizeRegion("region-c", TestClocks.Clock(2), "op");
        record.AuthorizeRegion("region-a", TestClocks.Clock(2), "op");
        record.AuthorizeRegion("region-b", TestClocks.Clock(2), "op");
        record.RevokeRegion("region-b", TestClocks.Clock(3), "op");

        Assert.That(record.AllowedRegionIds, Is.EqualTo(new[] { "region-a", "region-c" }));
    }

    [Test]
    public void SetRegionStatus_records_the_status()
    {
        var record = NewRecord();

        record.SetRegionStatus("region-a", TenantRegionStatus.Backfilling, TestClocks.Clock(2), "op");

        Assert.That(record.GetRegionStatus("region-a"), Is.EqualTo(TenantRegionStatus.Backfilling));
    }

    [Test]
    public void SetRegionStatus_with_an_older_stamp_is_ignored()
    {
        var record = NewRecord();
        record.SetRegionStatus("region-a", TenantRegionStatus.Online, TestClocks.Clock(5), "op");

        record.SetRegionStatus("region-a", TenantRegionStatus.Provisioning, TestClocks.Clock(2), "op");

        Assert.That(record.GetRegionStatus("region-a"), Is.EqualTo(TenantRegionStatus.Online));
    }

    [Test]
    public void GetRegionStatus_is_none_for_an_unknown_region()
    {
        var record = NewRecord();

        Assert.That(record.GetRegionStatus("region-x"), Is.EqualTo(TenantRegionStatus.None));
    }

    [Test]
    public void HasResidencyConfiguration_is_false_until_a_non_none_status_is_set()
    {
        var record = NewRecord();
        Assert.That(record.HasResidencyConfiguration, Is.False);

        record.SetRegionStatus("region-a", TenantRegionStatus.Provisioning, TestClocks.Clock(2), "op");

        Assert.That(record.HasResidencyConfiguration, Is.True);
    }

    [Test]
    public void ResidentRegionCount_counts_only_resident_statuses()
    {
        var record = NewRecord();
        record.SetRegionStatus("region-a", TenantRegionStatus.Online, TestClocks.Clock(2), "op");
        record.SetRegionStatus("region-b", TenantRegionStatus.Provisioning, TestClocks.Clock(2), "op");
        record.SetRegionStatus("region-c", TenantRegionStatus.Draining, TestClocks.Clock(2), "op");
        record.SetRegionStatus("region-d", TenantRegionStatus.Offline, TestClocks.Clock(2), "op");

        Assert.That(record.ResidentRegionCount, Is.EqualTo(2));
    }

    [Test]
    public void RegionStatusEntries_excludes_none_and_is_ordered()
    {
        var record = NewRecord();
        record.SetRegionStatus("region-b", TenantRegionStatus.Online, TestClocks.Clock(2), "op");
        record.SetRegionStatus("region-a", TenantRegionStatus.Draining, TestClocks.Clock(2), "op");
        record.SetRegionStatus("region-z", TenantRegionStatus.None, TestClocks.Clock(2), "op");

        Assert.That(record.RegionStatusEntries, Is.EqualTo(new[]
        {
            new KeyValuePair<string, TenantRegionStatus>("region-a", TenantRegionStatus.Draining),
            new KeyValuePair<string, TenantRegionStatus>("region-b", TenantRegionStatus.Online),
        }));
    }

    [TestCase(null)]
    [TestCase("")]
    public void Region_methods_reject_a_null_or_empty_region_id(string? regionId)
    {
        var record = NewRecord();

        Assert.Multiple(() =>
        {
            Assert.That(() => record.AuthorizeRegion(regionId!, TestClocks.Clock(2), "op"), Throws.InstanceOf<ArgumentException>());
            Assert.That(() => record.RevokeRegion(regionId!, TestClocks.Clock(2), "op"), Throws.InstanceOf<ArgumentException>());
            Assert.That(() => record.IsRegionAllowed(regionId!), Throws.InstanceOf<ArgumentException>());
            Assert.That(() => record.SetRegionStatus(regionId!, TenantRegionStatus.Online, TestClocks.Clock(2), "op"), Throws.InstanceOf<ArgumentException>());
            Assert.That(() => record.GetRegionStatus(regionId!), Throws.InstanceOf<ArgumentException>());
        });
    }

    [Test]
    public void Clone_copies_the_region_maps_independently()
    {
        var record = NewRecord();
        record.AuthorizeRegion("region-a", TestClocks.Clock(2), "op");
        record.SetRegionStatus("region-a", TenantRegionStatus.Online, TestClocks.Clock(2), "op");

        var clone = record.Clone();
        // Mutating the clone must not affect the original.
        clone.RevokeRegion("region-a", TestClocks.Clock(3), "op");
        clone.SetRegionStatus("region-a", TenantRegionStatus.Draining, TestClocks.Clock(3), "op");

        Assert.Multiple(() =>
        {
            Assert.That(record.IsRegionAllowed("region-a"), Is.True);
            Assert.That(record.GetRegionStatus("region-a"), Is.EqualTo(TenantRegionStatus.Online));
            Assert.That(clone.IsRegionAllowed("region-a"), Is.False);
            Assert.That(clone.GetRegionStatus("region-a"), Is.EqualTo(TenantRegionStatus.Draining));
        });
    }

    [Test]
    public void Merge_converges_concurrent_allowed_writes_from_two_replicas()
    {
        // Operator on replica 1 authorizes region-a; operator on replica 2 authorizes
        // region-b. After a symmetric merge both replicas hold both regions.
        var replica1 = NewRecord();
        replica1.AuthorizeRegion("region-a", TestClocks.Clock(10), "op-1");

        var replica2 = NewRecord();
        replica2.AuthorizeRegion("region-b", TestClocks.Clock(11), "op-2");

        var merged12 = TenantRecord.Merge(replica1, replica2);
        var merged21 = TenantRecord.Merge(replica2, replica1);

        Assert.Multiple(() =>
        {
            Assert.That(merged12.AllowedRegionIds, Is.EqualTo(new[] { "region-a", "region-b" }));
            Assert.That(merged21.AllowedRegionIds, Is.EqualTo(new[] { "region-a", "region-b" }));
        });
    }

    [Test]
    public void Merge_converges_concurrent_status_writes_on_the_winning_stamp()
    {
        // A tenant-admin residency edit and a lifecycle promotion for the same region
        // race; the higher stamp wins on both merge orders.
        var replica1 = NewRecord();
        replica1.SetRegionStatus("region-a", TenantRegionStatus.Backfilling, TestClocks.Clock(20), "region-a");

        var replica2 = NewRecord();
        replica2.SetRegionStatus("region-a", TenantRegionStatus.Online, TestClocks.Clock(21), "region-a");

        var merged12 = TenantRecord.Merge(replica1, replica2);
        var merged21 = TenantRecord.Merge(replica2, replica1);

        Assert.Multiple(() =>
        {
            Assert.That(merged12.GetRegionStatus("region-a"), Is.EqualTo(TenantRegionStatus.Online));
            Assert.That(merged21.GetRegionStatus("region-a"), Is.EqualTo(TenantRegionStatus.Online));
        });
    }

    [Test]
    public void Merge_keeps_operator_and_tenant_admin_writes_independent()
    {
        // The operator authorizes a region on one replica while the tenant admin sets a
        // status on another; both survive the merge because the two maps are stamped
        // independently.
        var operatorReplica = NewRecord();
        operatorReplica.AuthorizeRegion("region-a", TestClocks.Clock(10), "op");

        var adminReplica = NewRecord();
        adminReplica.SetRegionStatus("region-a", TenantRegionStatus.Provisioning, TestClocks.Clock(11), "admin");

        var merged = TenantRecord.Merge(operatorReplica, adminReplica);

        Assert.Multiple(() =>
        {
            Assert.That(merged.IsRegionAllowed("region-a"), Is.True);
            Assert.That(merged.GetRegionStatus("region-a"), Is.EqualTo(TenantRegionStatus.Provisioning));
        });
    }

    // ---- TryPromoteRegionStatus (issue #3897) ------------------------------

    [TestCase(TenantRegionStatus.Provisioning, TenantRegionStatus.Backfilling)]
    [TestCase(TenantRegionStatus.Backfilling, TenantRegionStatus.Online)]
    [TestCase(TenantRegionStatus.Draining, TenantRegionStatus.Offline)]
    [TestCase(TenantRegionStatus.Offline, TenantRegionStatus.Removed)]
    public void TryPromoteRegionStatus_applies_the_single_legal_step(TenantRegionStatus from, TenantRegionStatus to)
    {
        var record = NewRecord();
        record.SetRegionStatus("region-a", from, TestClocks.Clock(5), "admin");

        var promotedOk = record.TryPromoteRegionStatus("region-a", "driver", out var promoted);

        Assert.Multiple(() =>
        {
            Assert.That(promotedOk, Is.True);
            Assert.That(promoted, Is.EqualTo(to));
            Assert.That(record.GetRegionStatus("region-a"), Is.EqualTo(to));
        });
    }

    [TestCase(TenantRegionStatus.Online)]
    [TestCase(TenantRegionStatus.Removed)]
    [TestCase(TenantRegionStatus.None)]
    public void TryPromoteRegionStatus_is_a_no_op_at_a_terminal_or_non_transitional_status(TenantRegionStatus status)
    {
        var record = NewRecord();
        record.SetRegionStatus("region-a", status, TestClocks.Clock(5), "admin");

        var promotedOk = record.TryPromoteRegionStatus("region-a", "driver", out var promoted);

        Assert.Multiple(() =>
        {
            Assert.That(promotedOk, Is.False);
            Assert.That(promoted, Is.EqualTo(status));
            Assert.That(record.GetRegionStatus("region-a"), Is.EqualTo(status));
        });
    }

    [Test]
    public void TryPromoteRegionStatus_is_a_no_op_for_a_region_with_no_slot()
    {
        var record = NewRecord();

        var promotedOk = record.TryPromoteRegionStatus("region-a", "driver", out var promoted);

        Assert.Multiple(() =>
        {
            Assert.That(promotedOk, Is.False);
            Assert.That(promoted, Is.EqualTo(TenantRegionStatus.None));
            Assert.That(record.HasResidencyConfiguration, Is.False, "a no-op must not create a slot");
        });
    }

    [TestCase(null)]
    [TestCase("")]
    public void TryPromoteRegionStatus_null_or_empty_region_throws(string? regionId) =>
        Assert.That(
            () => NewRecord().TryPromoteRegionStatus(regionId!, "driver", out _),
            Throws.InstanceOf<ArgumentException>());

    [Test]
    public void A_promotion_supersedes_the_version_it_was_computed_from()
    {
        // Merging the promotion back into a replica that still holds the observed
        // version must apply it, whatever the writer ids.
        var stored = NewRecord();
        stored.SetRegionStatus("region-a", TenantRegionStatus.Draining, TestClocks.Clock(5), "zzz-admin");
        var promotion = stored.Clone();
        promotion.TryPromoteRegionStatus("region-a", "aaa-driver", out _);

        var merged = TenantRecord.Merge(stored, promotion);

        Assert.That(merged.GetRegionStatus("region-a"), Is.EqualTo(TenantRegionStatus.Offline));
    }

    [Test]
    public void A_promotion_never_supersedes_a_write_committed_after_its_read()
    {
        // The regression behind issue #3897's driver: the promoter reads Provisioning,
        // a tenant admin then drains the region, and the stale promotion is written
        // back. Stamped at the observed version's successor it must lose to the later
        // admin write, even one only a single tick later.
        var stored = NewRecord();
        stored.SetRegionStatus("region-a", TenantRegionStatus.Provisioning, TestClocks.Clock(5), "admin");
        var promotion = stored.Clone();
        stored.SetRegionStatus("region-a", TenantRegionStatus.Draining, TestClocks.Clock(6), "admin");

        promotion.TryPromoteRegionStatus("region-a", "zzz-driver", out _);
        var merged = TenantRecord.Merge(stored, promotion);

        Assert.That(merged.GetRegionStatus("region-a"), Is.EqualTo(TenantRegionStatus.Draining));
    }

    [Test]
    public void Two_promoters_of_the_same_version_converge_on_one_slot()
    {
        var stored = NewRecord();
        stored.SetRegionStatus("region-a", TenantRegionStatus.Draining, TestClocks.Clock(5), "admin");
        var first = stored.Clone();
        var second = stored.Clone();

        first.TryPromoteRegionStatus("region-a", "cluster", out _);
        second.TryPromoteRegionStatus("region-a", "cluster", out _);

        Assert.Multiple(() =>
        {
            Assert.That(
                TenantRecord.Merge(first, second).RegionStatusEntries,
                Is.EqualTo(first.RegionStatusEntries));
            Assert.That(first.GetRegionStatus("region-a"), Is.EqualTo(TenantRegionStatus.Offline));
        });
    }

    [Test]
    public void A_promotion_at_a_saturated_counter_rolls_to_the_next_wall_clock_tick()
    {
        var stored = NewRecord();
        stored.SetRegionStatus("region-a", TenantRegionStatus.Draining, TestClocks.Clock(5, int.MaxValue), "admin");
        var promotion = stored.Clone();

        var promotedOk = promotion.TryPromoteRegionStatus("region-a", "driver", out _);
        var laterAdmin = stored.Clone();
        laterAdmin.SetRegionStatus("region-a", TenantRegionStatus.Provisioning, TestClocks.Clock(7), "admin");

        Assert.Multiple(() =>
        {
            Assert.That(promotedOk, Is.True);
            Assert.That(TenantRecord.Merge(stored, promotion).GetRegionStatus("region-a"), Is.EqualTo(TenantRegionStatus.Offline));
            Assert.That(
                TenantRecord.Merge(promotion, laterAdmin).GetRegionStatus("region-a"),
                Is.EqualTo(TenantRegionStatus.Provisioning),
                "the rolled successor is still minimal: a later wall-clock write wins");
        });
    }

    [Test]
    public void A_promotion_at_the_representable_ceiling_writes_nothing()
    {
        var record = NewRecord();
        var ceiling = new HybridLogicalClock { WallClockTicks = long.MaxValue, Counter = int.MaxValue };
        record.SetRegionStatus("region-a", TenantRegionStatus.Draining, ceiling, "admin");

        var promotedOk = record.TryPromoteRegionStatus("region-a", "driver", out var promoted);

        Assert.Multiple(() =>
        {
            Assert.That(promotedOk, Is.False, "no successor exists, so the promotion must fail closed");
            Assert.That(promoted, Is.EqualTo(TenantRegionStatus.Draining));
        });
    }
}

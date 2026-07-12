namespace Orleans.Lattice.Backup.Tests;

/// <summary>
/// Regression coverage for <see cref="BackupManifestRegistration"/>: re-registering
/// a content-addressed backup id must preserve the immutable, index-key-determining
/// fields from the first registration, so a re-capture of identical content never
/// re-keys the catalog index into an orphaned duplicate row (the "one backup shows
/// as several rows" defect).
/// </summary>
[TestFixture]
public sealed class BackupManifestRegistrationTests
{
    private static BackupManifest Manifest(
        string id,
        DateTimeOffset createdAtUtc,
        string? setId = null,
        string? setName = null,
        DateTimeOffset? setCreatedAtUtc = null) =>
        BackupManifestModelTests.Sample(id: id) with
        {
            CreatedAtUtc = createdAtUtc,
            Scope = BackupScopeSelector.WholeTree("orders"),
            SetId = setId,
            SetName = setName,
            SetCreatedAtUtc = setCreatedAtUtc,
        };

    [Test]
    public void Reconcile_returns_incoming_verbatim_for_a_new_id()
    {
        var incoming = Manifest("new", DateTimeOffset.UnixEpoch);

        Assert.That(BackupManifestRegistration.Reconcile(existing: null, incoming), Is.SameAs(incoming));
    }

    [Test]
    public void Reconcile_preserves_the_first_created_timestamp_for_an_existing_id()
    {
        var firstSeen = new DateTimeOffset(2024, 1, 1, 0, 0, 0, TimeSpan.Zero);
        var existing = Manifest("cafef00d", firstSeen);

        // The same content re-captured later: same id, but a fresh capture time.
        var recaptured = Manifest("cafef00d", firstSeen.AddDays(3));

        var reconciled = BackupManifestRegistration.Reconcile(existing, recaptured);

        Assert.That(reconciled.CreatedAtUtc, Is.EqualTo(firstSeen));
    }

    [Test]
    public void Reconcile_allows_set_membership_to_be_stamped_on_an_existing_id()
    {
        // The multi-tree set capture re-registers each member to stamp the shared
        // set identity after the members are captured; that stamp must win, so
        // reconciliation keeps the incoming set membership (only the capture time
        // is carried forward).
        var setCreated = DateTimeOffset.UnixEpoch.AddHours(5);
        var existing = Manifest("m1", DateTimeOffset.UnixEpoch);

        var stamped = Manifest("m1", DateTimeOffset.UnixEpoch, setId: "set-1", setName: "nightly", setCreatedAtUtc: setCreated);

        var reconciled = BackupManifestRegistration.Reconcile(existing, stamped);

        Assert.Multiple(() =>
        {
            Assert.That(reconciled.SetId, Is.EqualTo("set-1"));
            Assert.That(reconciled.SetName, Is.EqualTo("nightly"));
            Assert.That(reconciled.SetCreatedAtUtc, Is.EqualTo(setCreated));
        });
    }

    [Test]
    public void Reconcile_keeps_the_index_key_stable_across_a_recapture()
    {
        var firstSeen = new DateTimeOffset(2024, 1, 1, 0, 0, 0, TimeSpan.Zero);
        var existing = Manifest("cafef00d", firstSeen);
        var recaptured = Manifest("cafef00d", firstSeen.AddDays(3));

        // Before the fix the re-capture's later timestamp would encode a *different*
        // index key, leaving the first key behind as an orphan row. After reconcile
        // both encode the same key, so the upsert lands in place.
        var reconciled = BackupManifestRegistration.Reconcile(existing, recaptured);

        Assert.That(
            BackupCatalogIndexKey.Encode(reconciled),
            Is.EqualTo(BackupCatalogIndexKey.Encode(existing)));
    }
}

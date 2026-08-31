using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Orleans.Lattice.GrainIndex.Registry;

/// <summary>
/// Reconciles every declared grain index against the registry at silo start:
/// audits the index's backing tree against the cluster's replication
/// configuration, then compares the live declaration against the record its
/// stored entries were written under and takes one of four branches.
/// </summary>
/// <remarks>
/// <para>
/// The four reconciliation branches, per index, are:
/// </para>
/// <list type="number">
/// <item>
/// <description>
/// <b>No stored record</b> - a first run. The declaration is persisted with its
/// fingerprint and the index is marked as needing a backfill, because no entry
/// has been written for it yet.
/// </description>
/// </item>
/// <item>
/// <description>
/// <b>Match</b> - the declaration is identical to the stored one. Nothing is
/// written and start proceeds.
/// </description>
/// </item>
/// <item>
/// <description>
/// <b>Drift confined to drift-safe fields</b> - the stored record is refreshed
/// to the new declaration, its needs-backfill state carried over unchanged, and
/// the change is logged at <see cref="LogLevel.Information"/>.
/// </description>
/// </item>
/// <item>
/// <description>
/// <b>Drift on a drift-breaking field</b> - under the default
/// <see cref="GrainIndexDriftPolicy.Reject"/> the silo fails with a
/// <see cref="GrainIndexConfigurationDriftException"/> naming the changed
/// fields; under the opt-in <see cref="GrainIndexDriftPolicy.Rebuild"/> the
/// stored record is updated with its needs-backfill flag raised, so the change
/// is accepted and a rebuild is scheduled instead of blocking the rollout.
/// </description>
/// </item>
/// </list>
/// <para>
/// The replication guard runs <i>before</i> the drift branches, so a silo whose
/// index tree is wrongly configured for cross-cluster replication is rejected
/// without first persisting a record it will not be allowed to use. The guard
/// <b>audits only</b>: it reads <see cref="ILatticeMergeModeResolver"/> and never
/// writes a merge mode back, so replication stays a deliberate, reversible
/// operator choice. A host with no replication package registered resolves the
/// core default, which returns <c>null</c> for every tree and makes the guard a
/// silent no-op.
/// </para>
/// <para>
/// Reconciliation is idempotent: running it twice against an unchanged
/// declaration writes nothing the second time, so every silo in a cluster can
/// run it at start.
/// </para>
/// </remarks>
internal sealed class GrainIndexRegistryReconciler
{
    private readonly IOptions<GrainIndexDeclarationOptions> _declarations;
    private readonly IOptionsMonitor<GrainIndexOptions> _indexOptions;
    private readonly IGrainIndexRegistryStore _store;
    private readonly ILatticeMergeModeResolver? _mergeModeResolver;
    private readonly ILogger<GrainIndexRegistryReconciler> _logger;

    /// <summary>Initialises the reconciler.</summary>
    /// <param name="declarations">The declared index set. Must not be <c>null</c>.</param>
    /// <param name="indexOptions">The per-index options, resolved by index name. Must not be <c>null</c>.</param>
    /// <param name="store">The registry store. Must not be <c>null</c>.</param>
    /// <param name="logger">The logger. Must not be <c>null</c>.</param>
    /// <param name="mergeModeResolver">
    /// The tree merge-mode resolver the replication guard audits. Optional: a
    /// host that registered no resolver at all disables the guard entirely,
    /// exactly as the core default resolver does by returning <c>null</c>.
    /// </param>
    /// <exception cref="ArgumentNullException">Any required argument is <c>null</c>.</exception>
    public GrainIndexRegistryReconciler(
        IOptions<GrainIndexDeclarationOptions> declarations,
        IOptionsMonitor<GrainIndexOptions> indexOptions,
        IGrainIndexRegistryStore store,
        ILogger<GrainIndexRegistryReconciler> logger,
        ILatticeMergeModeResolver? mergeModeResolver = null)
    {
        ArgumentNullException.ThrowIfNull(declarations);
        ArgumentNullException.ThrowIfNull(indexOptions);
        ArgumentNullException.ThrowIfNull(store);
        ArgumentNullException.ThrowIfNull(logger);
        _declarations = declarations;
        _indexOptions = indexOptions;
        _store = store;
        _logger = logger;
        _mergeModeResolver = mergeModeResolver;
    }

    /// <summary>
    /// Reconciles every declared index, in declaration order.
    /// </summary>
    /// <param name="cancellationToken">Cancels the reconciliation.</param>
    /// <exception cref="GrainIndexConfigurationDriftException">
    /// A declaration drifted on a drift-breaking field and its policy is
    /// <see cref="GrainIndexDriftPolicy.Reject"/>.
    /// </exception>
    /// <exception cref="GrainIndexReplicationNotAllowedException">
    /// An index's backing tree resolves to a replicated merge mode while the
    /// index has not opted in.
    /// </exception>
    /// <exception cref="InvalidOperationException">
    /// An index's backing tree name collides with the registry's own tree.
    /// </exception>
    public async Task ReconcileAsync(CancellationToken cancellationToken)
    {
        var definitions = _declarations.Value.Definitions;
        for (var i = 0; i < definitions.Count; i++)
        {
            await ReconcileOneAsync(definitions[i], cancellationToken).ConfigureAwait(false);
        }
    }

    private async Task ReconcileOneAsync(
        IGrainIndexDefinition definition,
        CancellationToken cancellationToken)
    {
        var indexName = definition.Name;
        var options = _indexOptions.Get(indexName);
        var descriptor = definition.Describe(options);
        var keyCodecId = GrainIndexKeyCodecIdentity.For(definition.KeyCodec);

        GuardRegistryTreeCollision(indexName, descriptor.TreeName);
        AuditReplication(indexName, descriptor.TreeName, options.AllowReplication);

        var fingerprint = GrainIndexFingerprint.Compute(descriptor, keyCodecId);
        var stored = await _store.ReadAsync(indexName, cancellationToken).ConfigureAwait(false);

        if (stored is null)
        {
            // Branch 1: first run. Nothing has been written under this index, so
            // the declaration is adopted as-is and the index owes a backfill.
            await _store
                .WriteAsync(
                    indexName,
                    new GrainIndexRegistryRecord(descriptor, keyCodecId, fingerprint, needsBackfill: true),
                    cancellationToken)
                .ConfigureAwait(false);

            _logger.LogInformation(
                "Grain index '{IndexName}' registered for the first time on tree '{TreeName}' "
                + "with fingerprint {Fingerprint}; it is marked as needing a backfill.",
                indexName,
                descriptor.TreeName,
                fingerprint.Value);
            return;
        }

        var report = GrainIndexDriftDetector.Detect(stored, descriptor, keyCodecId);
        if (!report.HasDrift)
        {
            // Branch 2: match. Deliberately no write, so a restart of an
            // unchanged silo touches the registry not at all.
            return;
        }

        var breaking = report.BreakingFields();
        if (breaking.Count == 0)
        {
            // Branch 3: drift confined to drift-safe fields. No stored entry
            // depends on them, so the record is refreshed and the index keeps
            // whatever backfill state it already had.
            await _store
                .WriteAsync(
                    indexName,
                    new GrainIndexRegistryRecord(
                        descriptor,
                        keyCodecId,
                        fingerprint,
                        stored.NeedsBackfill),
                    cancellationToken)
                .ConfigureAwait(false);

            _logger.LogInformation(
                "Grain index '{IndexName}' changed on drift-safe field(s) {ChangedFields}; the "
                + "registry record was updated and existing index data remains valid.",
                indexName,
                report.ChangedFields);
            return;
        }

        // Branch 4: drift on a drift-breaking field.
        if (options.DriftPolicy == GrainIndexDriftPolicy.Rebuild)
        {
            await _store
                .WriteAsync(
                    indexName,
                    new GrainIndexRegistryRecord(descriptor, keyCodecId, fingerprint, needsBackfill: true),
                    cancellationToken)
                .ConfigureAwait(false);

            _logger.LogWarning(
                "Grain index '{IndexName}' changed on drift-breaking field(s) {ChangedFields}. The "
                + "index's drift policy is {DriftPolicy}, so the new declaration was adopted and a "
                + "backfill rebuild scheduled. Queries against this index under-report until the "
                + "rebuild completes.",
                indexName,
                breaking,
                GrainIndexDriftPolicy.Rebuild);
            return;
        }

        throw new GrainIndexConfigurationDriftException(indexName, breaking);
    }

    /// <summary>
    /// Rejects a declaration whose backing tree is the registry's own tree,
    /// which would have the index write its entries over the bookkeeping that
    /// governs it.
    /// </summary>
    private static void GuardRegistryTreeCollision(string indexName, string treeName)
    {
        if (!string.Equals(treeName, GrainIndexRegistryTrees.RegistryTree, StringComparison.Ordinal))
        {
            return;
        }

        throw new InvalidOperationException(
            $"Grain index '{indexName}' is backed by tree '{treeName}', which is the grain-index "
            + "registry's own internal tree. The index would overwrite the bookkeeping that governs "
            + "it. Rename the index, or set an explicit tree name for it.");
    }

    /// <summary>
    /// Audits the index's backing tree against the cluster's replication
    /// configuration. Never writes a merge mode: a replicated tree that the
    /// index has opted in to is allowed and merely recorded.
    /// </summary>
    private void AuditReplication(string indexName, string treeName, bool allowReplication)
    {
        if (_mergeModeResolver?.Resolve(treeName) is not { } mergeMode)
        {
            // Either no resolver is registered, or the tree is not replicated.
            // Both make the guard a silent no-op.
            return;
        }

        if (!allowReplication)
        {
            throw new GrainIndexReplicationNotAllowedException(indexName, treeName, mergeMode);
        }

        _logger.LogInformation(
            "Grain index '{IndexName}' is backed by replicated tree '{TreeName}' with merge mode "
            + "{MergeMode}. The index opted in to replication, so the configuration is allowed; "
            + "note that entries name grain activations local to this cluster.",
            indexName,
            treeName,
            mergeMode);
    }
}

namespace Orleans.Lattice.Tests.Views;

[TestFixture]
public sealed class ILatticeViewFactoryTests
{
    private sealed class LegacyViewFactory : ILatticeViewFactory
    {
        public ILatticeView Create(
            ILattice source,
            string viewName,
            LatticeViewDefinition definition) =>
            throw new NotSupportedException();

        public Task<ILatticeView?> GetAsync(
            string viewName,
            CancellationToken cancellationToken = default) =>
            Task.FromResult<ILatticeView?>(null);

        public Task DeleteAsync(
            string viewName,
            CancellationToken cancellationToken = default) =>
            Task.CompletedTask;
    }

    [Test]
    public void Create_providerDescriptor_onLegacyImplementation_throwsNotSupported()
    {
        ILatticeViewFactory factory = new LegacyViewFactory();

        Assert.That(
            () => factory.Create(
                null!,
                "view",
                new LatticeRuntimeViewProjectionDescriptor("provider", [])),
            Throws.TypeOf<NotSupportedException>());
    }

    [Test]
    public void CreateAsync_definition_onLegacyImplementation_delegates_to_Create()
    {
        ILatticeViewFactory factory = new LegacyViewFactory();

        Assert.That(
            async () => await factory.CreateAsync(
                null!,
                "view",
                new LatticeViewDefinition("view", new PredicateLatticeViewProjection())),
            Throws.TypeOf<NotSupportedException>());
    }

    [Test]
    public void CreateAsync_providerDescriptor_onLegacyImplementation_delegates_to_Create()
    {
        ILatticeViewFactory factory = new LegacyViewFactory();

        Assert.That(
            async () => await factory.CreateAsync(
                null!,
                "view",
                new LatticeRuntimeViewProjectionDescriptor("provider", [])),
            Throws.TypeOf<NotSupportedException>());
    }

    [Test]
    public void CreateAsync_definition_onLegacyImplementation_honours_cancellation()
    {
        ILatticeViewFactory factory = new LegacyViewFactory();
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        Assert.That(
            async () => await factory.CreateAsync(
                null!,
                "view",
                new LatticeViewDefinition("view", new PredicateLatticeViewProjection()),
                cts.Token),
            Throws.TypeOf<OperationCanceledException>());
    }

    [Test]
    public void CreateAsync_providerDescriptor_onLegacyImplementation_honours_cancellation()
    {
        ILatticeViewFactory factory = new LegacyViewFactory();
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        Assert.That(
            async () => await factory.CreateAsync(
                null!,
                "view",
                new LatticeRuntimeViewProjectionDescriptor("provider", []),
                cts.Token),
            Throws.TypeOf<OperationCanceledException>());
    }
}

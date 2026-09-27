namespace Orleans.Lattice.Apps;

/// <summary>
/// One host registration of an <see cref="IAppChangeFeedHandler"/> for an (app, subscription) pair,
/// contributed to DI by <c>AddLatticeAppSubscriptionHandler</c> and collected by
/// <see cref="AppSubscriptionHandlerCatalog"/>.
/// </summary>
/// <param name="App">The app that declares the subscription.</param>
/// <param name="SubscriptionName">The manifest subscription name.</param>
/// <param name="Factory">Creates the handler; invoked at most once per catalog.</param>
internal sealed record AppSubscriptionHandlerRegistration(
    AppSlug App,
    string SubscriptionName,
    Func<IServiceProvider, IAppChangeFeedHandler> Factory);

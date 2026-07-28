using Amqp;
using Amqp.Sasl;
using Amqp.Types;

namespace AzsbEmu.Server.Protocol;

/// <summary>
/// Accepts the MSSBCBS SASL mechanism used by the Azure Service Bus SDK.
/// The emulator does not validate credentials — all connections are accepted.
/// Real authorization happens through the CBS token exchange after the AMQP
/// connection is established.
/// </summary>
internal sealed class MssbcbsSaslProfile : SaslProfile
{
    public MssbcbsSaslProfile() : base("MSSBCBS") { }

    protected override ITransport UpgradeTransport(ITransport transport) => transport;

#pragma warning disable CS8603
    protected override DescribedList GetStartCommand(string hostname) => null;
#pragma warning restore CS8603

    protected override DescribedList OnCommand(DescribedList command)
        => new SaslOutcome { Code = SaslCode.Ok };
}

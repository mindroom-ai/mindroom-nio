# Copyright © 2018 Damir Jelić <poljar@termina.org.uk>
#
# Permission to use, copy, modify, and/or distribute this software for
# any purpose with or without fee is hereby granted, provided that the
# above copyright notice and this permission notice appear in all copies.
#
# THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
# WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
# MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY
# SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER
# RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN ACTION OF
# CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF OR IN
# CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.


class ProtocolError(Exception):
    pass


class LocalProtocolError(ProtocolError):
    pass


class CallbackNotAcceptedError(Exception):
    """Signal that an event admission callback rejected an event.

    Raise this only from a callback registered with
    ``AsyncClient.add_event_admission_callback()``, before durable acceptance
    and before producing or scheduling side effects.
    Limited-timeline recovery keeps the event pending for redispatch.
    Raising this from an ordinary event callback is too late to reject the
    event and therefore has ordinary callback-error behavior.
    An ordinary error from a live callback acknowledges that event once, while
    an ordinary error from recovered history leaves it pending for a later
    pump or restart.
    """


class MembersSyncError(LocalProtocolError):
    pass


class SendRetryError(LocalProtocolError):
    pass


class RemoteProtocolError(ProtocolError):
    pass


class LocalTransportError(ProtocolError):
    pass


class RemoteTransportError(ProtocolError):
    pass


class OlmTrustError(Exception):
    pass


class OlmUnverifiedDeviceError(OlmTrustError):
    def __init__(self, unverified_device, *args):
        super().__init__(*args)
        self.device = unverified_device


class VerificationError(Exception):
    pass


class EncryptionError(Exception):
    pass


class GroupEncryptionError(Exception):
    pass


class TransferCancelledError(Exception):
    pass

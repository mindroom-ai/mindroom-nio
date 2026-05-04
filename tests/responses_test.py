import json
from pathlib import Path
from typing import Type

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from nio.responses import (
    ChangePasswordError,
    ChangePasswordResponse,
    DeleteDevicesAuthResponse,
    DevicesResponse,
    DiskDownloadResponse,
    DownloadError,
    DownloadResponse,
    ErrorResponse,
    JoinedMembersError,
    JoinedMembersResponse,
    JoinResponse,
    KeysClaimResponse,
    KeysQueryResponse,
    KeysUploadResponse,
    LoginError,
    LoginInfoResponse,
    LoginResponse,
    LogoutResponse,
    MemoryDownloadResponse,
    ProfileGetAvatarResponse,
    ProfileGetDisplayNameResponse,
    ProfileGetResponse,
    RegisterInteractiveResponse,
    RegisterResponse,
    RoomContextError,
    RoomContextResponse,
    RoomCreateResponse,
    RoomForgetResponse,
    RoomKeyRequestError,
    RoomKeyRequestResponse,
    RoomKnockResponse,
    RoomLeaveResponse,
    RoomMessagesResponse,
    RoomTypingResponse,
    SlidingSyncError,
    SlidingSyncResponse,
    SlidingSyncStateStub,
    SpaceGetHierarchyResponse,
    SyncError,
    SyncResponse,
    ThumbnailError,
    ThumbnailResponse,
    ToDeviceError,
    ToDeviceResponse,
    UploadResponse,
    _ErrorWithRoomId,
)

TEST_ROOM_ID = "!test:example.org"


def _load_response(filename):
    return json.loads(Path(filename).read_text())


class TestClass:
    def test_login_parse(self):
        parsed_dict = _load_response("tests/data/login_response.json")
        response = LoginResponse.from_dict(parsed_dict)
        assert isinstance(response, LoginResponse)

    def test_login_failure_parse(self):
        parsed_dict = _load_response("tests/data/login_response_error.json")
        response = LoginResponse.from_dict(parsed_dict)
        assert isinstance(response, LoginError)

    def test_login_failure_format(self):
        parsed_dict = _load_response("tests/data/login_invalid_format.json")
        response = LoginResponse.from_dict(parsed_dict)
        assert isinstance(response, ErrorResponse)

    def test_logout_parse(self):
        parsed_dict = _load_response("tests/data/logout_response.json")
        response = LogoutResponse.from_dict(parsed_dict)
        assert isinstance(response, LogoutResponse)

    def test_change_password_parse(self):
        parsed_dict = {}
        response = ChangePasswordResponse.from_dict(parsed_dict)
        assert isinstance(response, ChangePasswordResponse)

    def test_change_password_failure_parse(self):
        parsed_dict = {"errcode": "M_FORBIDDEN", "error": "Current password incorrect"}
        response = ChangePasswordResponse.from_dict(parsed_dict)
        assert isinstance(response, ChangePasswordError)
        assert response.status_code == "M_FORBIDDEN"
        assert response.message == "Current password incorrect"

    def test_room_messages(self):
        parsed_dict = _load_response("tests/data/room_messages.json")
        response = RoomMessagesResponse.from_dict(parsed_dict, TEST_ROOM_ID)
        assert isinstance(response, RoomMessagesResponse)

    def test_keys_upload(self):
        parsed_dict = _load_response("tests/data/keys_upload.json")
        response = KeysUploadResponse.from_dict(parsed_dict)
        assert isinstance(response, KeysUploadResponse)

    def test_keys_query(self):
        parsed_dict = _load_response("tests/data/keys_query.json")
        response = KeysQueryResponse.from_dict(parsed_dict)
        assert isinstance(response, KeysQueryResponse)

    def test_keys_claim(self):
        parsed_dict = _load_response("tests/data/keys_claim.json")
        response = KeysClaimResponse.from_dict(parsed_dict, "!test:example.org")
        assert isinstance(response, KeysClaimResponse)

    def test_devices(self):
        parsed_dict = _load_response("tests/data/devices.json")
        response = DevicesResponse.from_dict(parsed_dict)
        assert isinstance(response, DevicesResponse)
        assert response.devices[0].id == "QBUAZIFURK"

    def test_delete_devices_auth(self):
        parsed_dict = _load_response("tests/data/delete_devices.json")
        response = DeleteDevicesAuthResponse.from_dict(parsed_dict)
        assert isinstance(response, DeleteDevicesAuthResponse)
        assert response.session == "xxxxxxyz"

    def test_joined_parse(self):
        parsed_dict = _load_response("tests/data/joined_members_response.json")
        response = JoinedMembersResponse.from_dict(parsed_dict, "!testroom")
        assert isinstance(response, JoinedMembersResponse)

    def test_joined_fail(self):
        parsed_dict = {}
        response = JoinedMembersResponse.from_dict(parsed_dict, "!testroom")
        assert isinstance(response, JoinedMembersError)

    def test_upload_parse(self):
        parsed_dict = _load_response("tests/data/upload_response.json")
        response = UploadResponse.from_dict(parsed_dict)
        assert isinstance(response, UploadResponse)

    @pytest.mark.parametrize(
        ("data", "response_class"),
        [
            (Path("tests/data/file_response").read_bytes(), MemoryDownloadResponse),
            (Path("tests/data/file_response"), DiskDownloadResponse),
        ],
    )
    def test_download(self, data, response_class: Type[DownloadResponse]):
        response = response_class.from_data(data, "image/png", "example.png")
        assert isinstance(response, response_class)
        assert response.body == data
        assert response.content_type == "image/png"
        assert response.filename == "example.png"

        data = _load_response("tests/data/limit_exceeded_error.json")
        response = response_class.from_data(data, "image/png")
        assert isinstance(response, DownloadError)
        assert response.status_code == data["errcode"]

        response = response_class.from_data("123", "image/png")
        assert isinstance(response, DownloadError)

    def test_thumbnail(self):
        data = Path("tests/data/file_response").read_bytes()
        response = ThumbnailResponse.from_data(data, "image/png")
        assert isinstance(response, ThumbnailResponse)
        assert response.body == data

        data = _load_response("tests/data/limit_exceeded_error.json")
        response = ThumbnailResponse.from_data(data, "image/png")
        assert isinstance(response, ThumbnailError)
        assert response.status_code == data["errcode"]

        response = ThumbnailResponse.from_data("123", "image/png")
        assert isinstance(response, ThumbnailError)

        response = ThumbnailResponse.from_data(b"5xx error", "text/html")
        assert isinstance(response, ThumbnailError)

    def test_sync_fail(self):
        parsed_dict = {}
        response = SyncResponse.from_dict(parsed_dict, 0)
        assert isinstance(response, SyncError)

    def test_sync_parse(self):
        parsed_dict = _load_response("tests/data/sync.json")
        response = SyncResponse.from_dict(parsed_dict)
        assert isinstance(response, SyncResponse)

    def test_sliding_sync_fail(self):
        parsed_dict = {
            "errcode": "M_UNKNOWN_POS",
            "error": "Unknown sliding sync pos",
        }
        response = SlidingSyncResponse.from_dict(parsed_dict)
        assert isinstance(response, SlidingSyncError)

    def test_sliding_sync_minimal_response(self):
        response = SlidingSyncResponse.from_dict({"pos": "s1"})

        assert isinstance(response, SlidingSyncResponse)
        assert response.pos == "s1"
        assert response.lists == {}
        assert response.rooms == {}
        assert response.extensions == {}

    def test_sliding_sync_malformed_list_returns_error(self):
        response = SlidingSyncResponse.from_dict({"pos": "s1", "lists": {"main": {}}})

        assert isinstance(response, SlidingSyncError)

    def test_sliding_sync_malformed_room_returns_error(self):
        response = SlidingSyncResponse.from_dict(
            {
                "pos": "s1",
                "rooms": {
                    "!room:example.org": {
                        "required_state": [{"type": "m.room.name"}],
                    },
                },
            }
        )

        assert isinstance(response, SlidingSyncError)
        assert "!room:example.org" in response.message

    def test_sliding_sync_parse(self):
        parsed_dict = {
            "pos": "s58_224_0_13_10_1_1_16_0_1",
            "lists": {"main": {"count": 1}},
            "rooms": {
                "!room:example.org": {
                    "name": "Alice and Bob",
                    "avatar": None,
                    "heroes": [
                        {
                            "user_id": "@alice:example.org",
                            "displayname": "Alice",
                            "avatar_url": "mxc://example.org/alice",
                        }
                    ],
                    "is_dm": True,
                    "initial": True,
                    "unstable_expanded_timeline": True,
                    "required_state": [
                        {"type": "m.room.name", "state_key": ""},
                        {
                            "event_id": "$create:example.org",
                            "sender": "@alice:example.org",
                            "type": "m.room.create",
                            "state_key": "",
                            "origin_server_ts": 1,
                            "content": {"room_version": "12"},
                        },
                    ],
                    "timeline": [
                        {
                            "event_id": "$message:example.org",
                            "sender": "@alice:example.org",
                            "type": "m.room.message",
                            "origin_server_ts": 2,
                            "content": {"msgtype": "m.text", "body": "hi"},
                        }
                    ],
                    "prev_batch": "t111_222_333",
                    "limited": True,
                    "num_live": 1,
                    "joined_count": 2,
                    "invited_count": 0,
                    "notification_count": 11,
                    "highlight_count": 1,
                    "membership": "join",
                    "lists": ["main"],
                }
            },
            "extensions": {"account_data": {"foo": "bar"}},
        }

        response = SlidingSyncResponse.from_dict(parsed_dict)

        assert isinstance(response, SlidingSyncResponse)
        assert response.pos == "s58_224_0_13_10_1_1_16_0_1"
        assert response.lists["main"].count == 1
        assert response.extensions == {"account_data": {"foo": "bar"}}

        room = response.rooms["!room:example.org"]
        assert room.name == "Alice and Bob"
        assert room.avatar is None
        assert room.heroes[0].user_id == "@alice:example.org"
        assert room.is_dm
        assert room.initial
        assert room.expanded_timeline
        assert room.membership == "join"
        assert room.lists == ["main"]
        assert isinstance(room.required_state[0], SlidingSyncStateStub)
        assert room.required_state[0].type == "m.room.name"
        assert room.timeline[0].source["content"]["body"] == "hi"
        assert room.prev_batch == "t111_222_333"
        assert room.limited
        assert room.num_live == 1
        assert room.joined_count == 2
        assert room.invited_count == 0
        assert room.notification_count == 11
        assert room.highlight_count == 1

    _fuzz_json = st.recursive(
        st.none()
        | st.booleans()
        | st.integers()
        | st.floats(allow_nan=False)
        | st.text(max_size=12),
        lambda children: st.lists(children, max_size=4)
        | st.dictionaries(st.text(max_size=8), children, max_size=4),
        max_leaves=12,
    )

    @given(payload=st.dictionaries(st.text(max_size=12), _fuzz_json, max_size=6))
    @settings(max_examples=300, deadline=None)
    def test_sliding_sync_fuzz_never_raises(self, payload):
        response = SlidingSyncResponse.from_dict(payload)
        assert isinstance(response, (SlidingSyncResponse, ErrorResponse))

    @given(room=_fuzz_json, sync_list=_fuzz_json, extensions=_fuzz_json)
    @settings(max_examples=300, deadline=None)
    def test_sliding_sync_fuzz_nested_never_raises(self, room, sync_list, extensions):
        # A valid envelope forces parsing deep into rooms/lists/extensions.
        payload = {
            "pos": "p",
            "rooms": {"!fuzz:example.org": room},
            "lists": {"fuzz": sync_list},
            "extensions": extensions,
        }
        response = SlidingSyncResponse.from_dict(payload)
        assert isinstance(response, (SlidingSyncResponse, ErrorResponse))

    def test_sliding_sync_parse_stripped_state(self):
        # Deployed servers send invite_state; the current MSC4186 text
        # renamed it to stripped_state. Both must parse.
        for wire_key in ("invite_state", "stripped_state"):
            parsed_dict = {
                "pos": "s1",
                "rooms": {
                    "!invited:example.org": {
                        "membership": "invite",
                        wire_key: [
                            {
                                "sender": "@alice:example.org",
                                "state_key": "@bob:example.org",
                                "type": "m.room.member",
                                "content": {"membership": "invite"},
                            }
                        ],
                    }
                },
            }

            response = SlidingSyncResponse.from_dict(parsed_dict)

            assert isinstance(response, SlidingSyncResponse)
            room = response.rooms["!invited:example.org"]
            assert room.membership == "invite"
            assert room.stripped_state[0].source["type"] == "m.room.member"
            assert room.stripped_state[0].membership == "invite"

    def test_keyshare_request(self):
        parsed_dict = {
            "errcode": "M_LIMIT_EXCEEDED",
            "error": "Too many requests",
            "retry_after_ms": 2000,
        }
        response = RoomKeyRequestResponse.from_dict(
            parsed_dict, "1", "1", TEST_ROOM_ID, "megolm.v1"
        )
        assert isinstance(response, RoomKeyRequestError)
        response = RoomKeyRequestResponse.from_dict(
            {}, "1", "1", TEST_ROOM_ID, "megolm.v1"
        )
        assert isinstance(response, RoomKeyRequestResponse)

    def test_get_profile(self):
        parsed_dict = _load_response("tests/data/get_profile_response.json")
        response = ProfileGetResponse.from_dict(parsed_dict)
        assert isinstance(response, ProfileGetResponse)
        assert response.other_info == {"something_else": 123}

    def test_get_displayname(self):
        parsed_dict = _load_response("tests/data/get_displayname_response.json")
        response = ProfileGetDisplayNameResponse.from_dict(parsed_dict)
        assert isinstance(response, ProfileGetDisplayNameResponse)

    def test_get_avatar(self):
        parsed_dict = _load_response("tests/data/get_avatar_response.json")
        response = ProfileGetAvatarResponse.from_dict(parsed_dict)
        assert isinstance(response, ProfileGetAvatarResponse)

    def test_to_device(self):
        message = "message"
        response = ToDeviceResponse.from_dict(
            {"error": "error", "errcode": "M_UNKNOWN"}, message
        )
        assert isinstance(response, ToDeviceError)
        response = ToDeviceResponse.from_dict({}, message)
        assert isinstance(response, ToDeviceResponse)

    def test_context(self):
        response = RoomContextResponse.from_dict(
            {"error": "error", "errcode": "M_UNKNOWN"}, TEST_ROOM_ID
        )
        assert isinstance(response, RoomContextError)
        assert response.room_id == TEST_ROOM_ID

        parsed_dict = _load_response("tests/data/context.json")
        response = RoomContextResponse.from_dict(parsed_dict, TEST_ROOM_ID)

        assert isinstance(response, RoomContextResponse)

        assert response.room_id == TEST_ROOM_ID
        assert not response.events_before
        assert len(response.events_after) == 1
        assert len(response.state) == 9

    def test_limit_exceeded_error(self):
        parsed_dict = _load_response("tests/data/limit_exceeded_error.json")

        response = ErrorResponse.from_dict(parsed_dict)
        assert isinstance(response, ErrorResponse)
        assert response.retry_after_ms == parsed_dict["retry_after_ms"]

        room_id = "!SVkFJHzfwvuaIEawgC:localhost"
        response2 = _ErrorWithRoomId.from_dict(parsed_dict, room_id)
        assert isinstance(response2, _ErrorWithRoomId)
        assert response.retry_after_ms == parsed_dict["retry_after_ms"]
        assert response2.room_id == room_id

    def test_room_create(self):
        parsed_dict = _load_response("tests/data/room_id.json")
        response = RoomCreateResponse.from_dict(parsed_dict)
        assert isinstance(response, RoomCreateResponse)

    def test_join(self):
        parsed_dict = _load_response("tests/data/room_id.json")
        response = JoinResponse.from_dict(parsed_dict)
        assert isinstance(response, JoinResponse)

    def test_knock(self):
        parsed_dict = _load_response("tests/data/room_id.json")
        response = RoomKnockResponse.from_dict(parsed_dict)
        assert isinstance(response, RoomKnockResponse)

    def test_room_leave(self):
        response = RoomLeaveResponse.from_dict({})
        assert isinstance(response, RoomLeaveResponse)

    def test_room_forget(self):
        response = RoomForgetResponse.from_dict({}, TEST_ROOM_ID)
        assert isinstance(response, RoomForgetResponse)

    def test_room_typing(self):
        response = RoomTypingResponse.from_dict({}, TEST_ROOM_ID)
        assert isinstance(response, RoomTypingResponse)

    def test_login_info(self):
        parsed_dict = _load_response("tests/data/login_info.json")
        response = LoginInfoResponse.from_dict(parsed_dict)
        assert isinstance(response, LoginInfoResponse)

    def test_space_get_hierarchy(self):
        parsed_dict = _load_response("tests/data/get_hierarchy_response.json")
        response = SpaceGetHierarchyResponse.from_dict(parsed_dict)
        assert isinstance(response, SpaceGetHierarchyResponse)

    def test_register(self):
        parsed_dict = _load_response("tests/data/register_response.json")
        response = RegisterResponse.from_dict(parsed_dict)
        assert isinstance(response, RegisterResponse)

    def test_register_interactive(self):
        parsed_dict = _load_response("tests/data/register_interactive_response.json")
        response = RegisterInteractiveResponse.from_dict(parsed_dict)
        assert isinstance(response, RegisterInteractiveResponse)

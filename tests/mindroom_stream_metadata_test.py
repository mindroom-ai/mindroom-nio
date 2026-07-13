from nio.client.base_client import _copy_mindroom_stream_push_metadata


def test_stream_push_metadata_exposes_only_status_and_message_type():
    content = {
        "body": "private answer text",
        "io.mindroom.stream_status": "streaming",
        "m.mentions": {"user_ids": ["@private:example.org"]},
        "msgtype": "m.notice",
    }
    encrypted_content = {"algorithm": "m.megolm.v1.aes-sha2"}

    _copy_mindroom_stream_push_metadata(content, encrypted_content)

    assert encrypted_content == {
        "algorithm": "m.megolm.v1.aes-sha2",
        "io.mindroom.stream_status": "streaming",
        "msgtype": "m.notice",
    }


def test_non_stream_message_type_stays_encrypted():
    encrypted_content = {"algorithm": "m.megolm.v1.aes-sha2"}

    _copy_mindroom_stream_push_metadata(
        {"body": "private answer text", "msgtype": "m.text"},
        encrypted_content,
    )

    assert encrypted_content == {"algorithm": "m.megolm.v1.aes-sha2"}

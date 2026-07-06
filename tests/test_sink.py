import json
import pytest
from unittest.mock import MagicMock, patch

from botocore.exceptions import BotoCoreError, ClientError

from ib_stream.config import SinkConfig, SQSConfig
from ib_stream.sink import (
    KinesisSink,
    SQSSink,
    ThreadSink,
    create_sink,
)

_STANDARD_URL = "https://sqs.us-east-1.amazonaws.com/123456789012/ib-data"
_FIFO_URL = "https://sqs.us-east-1.amazonaws.com/123456789012/ib-data.fifo"


@pytest.fixture
def mock_boto3():
    with patch("ib_stream.sink.boto3") as m:
        m.client.return_value = MagicMock()
        yield m


@pytest.fixture
def standard_sink(mock_boto3):
    return SQSSink(region="us-east-1", queue_url=_STANDARD_URL)


@pytest.fixture
def fifo_sink(mock_boto3):
    return SQSSink(region="us-east-1", queue_url=_FIFO_URL)


class TestSQSSinkInit:
    def test_detects_standard_queue(self, mock_boto3):
        sink = SQSSink(region="us-east-1", queue_url=_STANDARD_URL)
        assert sink._is_fifo is False

    def test_detects_fifo_queue(self, mock_boto3):
        sink = SQSSink(region="us-east-1", queue_url=_FIFO_URL)
        assert sink._is_fifo is True

    def test_creates_boto3_client_with_region(self, mock_boto3):
        SQSSink(region="eu-west-1", queue_url=_STANDARD_URL)
        mock_boto3.client.assert_called_once_with("sqs", region_name="eu-west-1")

    def test_from_config(self, mock_boto3):
        cfg = SQSConfig(region="us-west-2", queue_url=_FIFO_URL)
        sink = SQSSink.from_config(cfg)
        assert sink._queue_url == _FIFO_URL
        assert sink._is_fifo is True


class TestPublishStandardQueue:
    def test_sends_message_body_as_json(self, standard_sink, mock_boto3):
        client = mock_boto3.client.return_value
        standard_sink.publish("AAPL:STK", {"bid": 150.5, "ask": 150.6})
        kwargs = client.send_message.call_args[1]
        parsed = json.loads(kwargs["MessageBody"])
        assert parsed["bid"] == 150.5
        assert parsed["ask"] == 150.6

    def test_sends_correct_queue_url(self, standard_sink, mock_boto3):
        client = mock_boto3.client.return_value
        standard_sink.publish("AAPL:STK", {})
        kwargs = client.send_message.call_args[1]
        assert kwargs["QueueUrl"] == _STANDARD_URL

    def test_no_message_group_id_for_standard(self, standard_sink, mock_boto3):
        client = mock_boto3.client.return_value
        standard_sink.publish("AAPL:STK", {})
        kwargs = client.send_message.call_args[1]
        assert "MessageGroupId" not in kwargs
        assert "MessageDeduplicationId" not in kwargs

    def test_logs_debug_on_success(self, standard_sink, mock_boto3):
        with patch("ib_stream.sink.logger") as mock_log:
            standard_sink.publish("AAPL:STK", {"bid": 1.0})
            mock_log.debug.assert_called_once()
            assert "AAPL:STK" in mock_log.debug.call_args[0][1]


class TestPublishFifoQueue:
    def test_sets_message_group_id_to_key(self, fifo_sink, mock_boto3):
        client = mock_boto3.client.return_value
        fifo_sink.publish("ES:FUT:202509", {"close": 5000.0})
        kwargs = client.send_message.call_args[1]
        assert kwargs["MessageGroupId"] == "ES:FUT:202509"

    def test_sets_unique_dedup_id_per_call(self, fifo_sink, mock_boto3):
        client = mock_boto3.client.return_value
        fifo_sink.publish("AAPL:STK", {})
        fifo_sink.publish("AAPL:STK", {})
        calls = client.send_message.call_args_list
        id1 = calls[0][1]["MessageDeduplicationId"]
        id2 = calls[1][1]["MessageDeduplicationId"]
        assert id1 != id2


class TestPublishErrorHandling:
    def test_warns_on_client_error_without_raising(self, standard_sink, mock_boto3):
        client = mock_boto3.client.return_value
        client.send_message.side_effect = ClientError(
            {"Error": {"Code": "500", "Message": "Service Unavailable"}}, "SendMessage"
        )
        with patch("ib_stream.sink.logger") as mock_log:
            standard_sink.publish("AAPL:STK", {})  # must not raise
        mock_log.warning.assert_called_once()
        assert "AAPL:STK" in mock_log.warning.call_args[0][1]

    def test_warns_on_botocore_error_without_raising(self, standard_sink, mock_boto3):
        client = mock_boto3.client.return_value
        client.send_message.side_effect = BotoCoreError()
        with patch("ib_stream.sink.logger") as mock_log:
            standard_sink.publish("AAPL:STK", {})  # must not raise
        mock_log.warning.assert_called_once()

    def test_non_serialisable_values_use_str_fallback(self, standard_sink, mock_boto3):
        client = mock_boto3.client.return_value
        from datetime import datetime
        standard_sink.publish("AAPL:STK", {"time": datetime(2024, 1, 1)})
        kwargs = client.send_message.call_args[1]
        # datetime is not JSON-serialisable by default; default=str must handle it
        parsed = json.loads(kwargs["MessageBody"])
        assert "2024" in parsed["time"]


class TestSQSFromOptions:
    def test_builds_from_options_dict(self, mock_boto3):
        sink = SQSSink.from_options({"region": "us-east-1", "queue_url": _FIFO_URL})
        assert isinstance(sink, SQSSink)
        assert sink._is_fifo is True


class TestKinesisSink:
    def test_creates_client_with_region(self, mock_boto3):
        KinesisSink.from_options({"region": "eu-west-1", "stream_name": "s"})
        mock_boto3.client.assert_called_once_with("kinesis", region_name="eu-west-1")

    def test_publish_uses_key_as_partition_key(self, mock_boto3):
        client = mock_boto3.client.return_value
        sink = KinesisSink.from_options({"region": "us-east-1", "stream_name": "ib-data"})
        sink.publish("ES:FUT:202509", {"close": 5000.0})
        kwargs = client.put_record.call_args[1]
        assert kwargs["StreamName"] == "ib-data"
        assert kwargs["PartitionKey"] == "ES:FUT:202509"
        assert json.loads(kwargs["Data"].decode())["close"] == 5000.0

    def test_publish_warns_on_error_without_raising(self, mock_boto3):
        client = mock_boto3.client.return_value
        client.put_record.side_effect = BotoCoreError()
        sink = KinesisSink.from_options({"region": "us-east-1", "stream_name": "s"})
        with patch("ib_stream.sink.logger") as mock_log:
            sink.publish("AAPL:STK", {})  # must not raise
        mock_log.warning.assert_called_once()


class TestThreadSink:
    def test_publish_enqueues_message(self):
        sink = ThreadSink.from_options({})
        sink.publish("AAPL:STK", {"bid": 1.0})
        key, payload = sink.get_nowait()
        assert key == "AAPL:STK"
        assert payload == {"bid": 1.0}

    def test_messages_property_snapshots_queue(self):
        sink = ThreadSink()
        sink.publish("A", {"x": 1})
        sink.publish("B", {"y": 2})
        assert sink.messages == [("A", {"x": 1}), ("B", {"y": 2})]

    def test_drops_when_full(self):
        sink = ThreadSink(maxsize=1)
        sink.publish("A", {})
        with patch("ib_stream.sink.logger") as mock_log:
            sink.publish("B", {})  # queue full — dropped, must not raise
        mock_log.warning.assert_called_once()


class TestCreateSink:
    def test_creates_sqs(self, mock_boto3):
        cfg = SinkConfig(id="q", type="sqs", options={"region": "us-east-1", "queue_url": _FIFO_URL})
        assert isinstance(create_sink(cfg), SQSSink)

    def test_creates_kinesis(self, mock_boto3):
        cfg = SinkConfig(id="k", type="kinesis", options={"region": "us-east-1", "stream_name": "s"})
        assert isinstance(create_sink(cfg), KinesisSink)

    def test_creates_thread(self):
        cfg = SinkConfig(id="local", type="thread")
        assert isinstance(create_sink(cfg), ThreadSink)

    def test_unknown_type_raises_value_error(self):
        cfg = SinkConfig(id="x", type="bogus")
        with pytest.raises(ValueError, match="Unknown sink type"):
            create_sink(cfg)

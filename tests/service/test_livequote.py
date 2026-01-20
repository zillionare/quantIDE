import unittest
from unittest.mock import MagicMock, patch
import msgpack
from pyqmt.service.livequote import LiveQuote


class TestLiveQuote(unittest.TestCase):
    def setUp(self):
        # 模拟配置
        self.mock_cfg = MagicMock()
        self.mock_cfg.server.get.side_effect = lambda k, d: d
        self.mock_cfg.get.return_value = None

        with patch("cfg4py.get_instance", return_value=self.mock_cfg):
            self.live_quote = LiveQuote()

    def test_cache_and_broadcast(self):
        """测试数据缓存和广播逻辑"""
        test_data = {"600000.SH": {"lastPrice": 7.5}}

        with patch("pyqmt.service.livequote.msg_hub") as mock_hub:
            self.live_quote._cache_and_broadcast(test_data)

            # 验证缓存更新
            self.assertEqual(self.live_quote.get_quote("600000.SH")["lastPrice"], 7.5)
            # 验证消息广播
            mock_hub.publish.assert_called_once_with("quote.all", test_data)

    def test_on_redis_message_msgpack(self):
        """测试解析正确的 Msgpack 数据"""
        test_dict = {"000001.SZ": {"lastPrice": 10.0}}
        raw_bytes = msgpack.packb(test_dict)

        with patch.object(self.live_quote, "_cache_and_broadcast") as mock_broadcast:
            self.live_quote._on_redis_message(raw_bytes)
            mock_broadcast.assert_called_once_with(test_dict)

    def test_on_redis_message_invalid(self):
        """测试处理非法的 Msgpack 数据"""
        invalid_bytes = b"not a msgpack"

        with patch("pyqmt.service.livequote.logger") as mock_logger:
            self.live_quote._on_redis_message(invalid_bytes)
            # 验证记录了错误日志，且没有崩溃
            mock_logger.error.assert_called()


if __name__ == "__main__":
    unittest.main()

import unittest
from unittest.mock import Mock, patch, call
import pika
import sys
import os

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

try:
    from rabbitmq.middleware import (
        MessageMiddlewareQueue,
        MessageMiddlewareExchange,
        MessageMiddlewareMessageError,
        MessageMiddlewareDisconnectedError,
        MessageMiddlewareCloseError,
        MessageMiddlewareDeleteError
    )
except ImportError:
    middleware_path = os.path.join(project_root, 'rabbitmq')
    sys.path.insert(0, middleware_path)
    from middleware import (
        MessageMiddlewareQueue,
        MessageMiddlewareExchange,
        MessageMiddlewareMessageError,
        MessageMiddlewareDisconnectedError,
        MessageMiddlewareCloseError,
        MessageMiddlewareDeleteError
    )


class TestMessageMiddlewareQueue(unittest.TestCase):
    """Test suite for MessageMiddlewareQueue class"""

    def setUp(self):
        """Set up test fixtures before each test method."""
        self.host = "localhost"
        self.queue_name = "test_queue"
        self.mock_connection = Mock()
        self.mock_channel = Mock()
        self.mock_connection.channel.return_value = self.mock_channel

    @patch('rabbitmq.middleware.pika.BlockingConnection')
    def test_init_success(self, mock_blocking_connection):
        """Test successful initialization of MessageMiddlewareQueue"""
        mock_blocking_connection.return_value = self.mock_connection
        
        middleware = MessageMiddlewareQueue(self.host, self.queue_name)
        
        self.assertEqual(middleware.host, self.host)
        self.assertEqual(middleware.queue_name, self.queue_name)
        self.assertEqual(middleware.connection, self.mock_connection)
        self.assertEqual(middleware.channel, self.mock_channel)
        
        # Verify connection setup
        mock_blocking_connection.assert_called_once()
        self.mock_channel.queue_declare.assert_called_once_with(queue=self.queue_name, durable=True)

    @patch('rabbitmq.middleware.pika.BlockingConnection')
    def test_init_connection_error(self, mock_blocking_connection):
        """Test initialization failure due to connection error"""
        mock_blocking_connection.side_effect = pika.exceptions.AMQPConnectionError("Connection failed")
        
        with self.assertRaises(MessageMiddlewareDisconnectedError):
            MessageMiddlewareQueue(self.host, self.queue_name)

    @patch('rabbitmq.middleware.pika.BlockingConnection')
    def test_send_message_success(self, mock_blocking_connection):
        """Test successful message sending"""
        mock_blocking_connection.return_value = self.mock_connection
        middleware = MessageMiddlewareQueue(self.host, self.queue_name)
        
        test_message = "test message"
        middleware.send(test_message)
        
        self.mock_channel.basic_publish.assert_called_once_with(
            exchange='',
            routing_key=self.queue_name,
            body=test_message,
            properties=unittest.mock.ANY
        )

    @patch('rabbitmq.middleware.pika.BlockingConnection')
    def test_send_message_no_connection(self, mock_blocking_connection):
        """Test sending message when there's no active connection"""
        mock_blocking_connection.return_value = self.mock_connection
        middleware = MessageMiddlewareQueue(self.host, self.queue_name)
        middleware.channel = None
        
        with self.assertRaises(MessageMiddlewareDisconnectedError):
            middleware.send("test message")

    @patch('rabbitmq.middleware.pika.BlockingConnection')
    def test_send_message_publish_error(self, mock_blocking_connection):
        """Test sending message when publish fails"""
        mock_blocking_connection.return_value = self.mock_connection
        self.mock_channel.basic_publish.side_effect = Exception("Publish failed")
        middleware = MessageMiddlewareQueue(self.host, self.queue_name)
        
        with self.assertRaises(MessageMiddlewareMessageError):
            middleware.send("test message")

    @patch('rabbitmq.middleware.pika.BlockingConnection')
    def test_start_consuming_success(self, mock_blocking_connection):
        """Test successful start of message consumption"""
        mock_blocking_connection.return_value = self.mock_connection
        middleware = MessageMiddlewareQueue(self.host, self.queue_name)
        
        callback = Mock()
        middleware.start_consuming(callback)
        
        self.mock_channel.basic_qos.assert_called_once_with(prefetch_count=10)
        self.mock_channel.basic_consume.assert_called_once()
        self.mock_channel.start_consuming.assert_called_once()

    @patch('rabbitmq.middleware.pika.BlockingConnection')
    def test_start_consuming_no_connection(self, mock_blocking_connection):
        """Test start consuming when there's no active connection"""
        mock_blocking_connection.return_value = self.mock_connection
        middleware = MessageMiddlewareQueue(self.host, self.queue_name)
        middleware.channel = None
        
        callback = Mock()
        with self.assertRaises(MessageMiddlewareDisconnectedError):
            middleware.start_consuming(callback)

    @patch('rabbitmq.middleware.pika.BlockingConnection')
    def test_start_consuming_connection_error(self, mock_blocking_connection):
        """Test start consuming when connection is lost"""
        mock_blocking_connection.return_value = self.mock_connection
        self.mock_channel.start_consuming.side_effect = pika.exceptions.AMQPConnectionError("Connection lost")
        middleware = MessageMiddlewareQueue(self.host, self.queue_name)
        
        callback = Mock()
        with self.assertRaises(MessageMiddlewareDisconnectedError):
            middleware.start_consuming(callback)

    @patch('rabbitmq.middleware.pika.BlockingConnection')
    def test_stop_consuming_success(self, mock_blocking_connection):
        """Test successful stop of message consumption"""
        mock_blocking_connection.return_value = self.mock_connection
        middleware = MessageMiddlewareQueue(self.host, self.queue_name)
        
        middleware.stop_consuming()
        
        self.mock_channel.stop_consuming.assert_called_once()

    @patch('rabbitmq.middleware.pika.BlockingConnection')
    def test_stop_consuming_error(self, mock_blocking_connection):
        """Test stop consuming when an error occurs"""
        mock_blocking_connection.return_value = self.mock_connection
        self.mock_channel.stop_consuming.side_effect = Exception("Stop failed")
        middleware = MessageMiddlewareQueue(self.host, self.queue_name)
        
        with self.assertRaises(MessageMiddlewareMessageError):
            middleware.stop_consuming()

    @patch('rabbitmq.middleware.pika.BlockingConnection')
    def test_delete_queue_success(self, mock_blocking_connection):
        """Test successful queue deletion"""
        mock_blocking_connection.return_value = self.mock_connection
        middleware = MessageMiddlewareQueue(self.host, self.queue_name)
        
        middleware.delete()
        
        self.mock_channel.queue_delete.assert_called_once_with(queue=self.queue_name)

    @patch('rabbitmq.middleware.pika.BlockingConnection')
    def test_delete_queue_error(self, mock_blocking_connection):
        """Test queue deletion when an error occurs"""
        mock_blocking_connection.return_value = self.mock_connection
        self.mock_channel.queue_delete.side_effect = Exception("Delete failed")
        middleware = MessageMiddlewareQueue(self.host, self.queue_name)
        
        with self.assertRaises(MessageMiddlewareDeleteError):
            middleware.delete()

    @patch('rabbitmq.middleware.pika.BlockingConnection')
    def test_close_connection_success(self, mock_blocking_connection):
        """Test successful connection closure"""
        mock_blocking_connection.return_value = self.mock_connection
        middleware = MessageMiddlewareQueue(self.host, self.queue_name)
        
        middleware.close()
        
        self.mock_connection.close.assert_called_once()

    @patch('rabbitmq.middleware.pika.BlockingConnection')
    def test_close_connection_error(self, mock_blocking_connection):
        """Test connection closure when an error occurs"""
        mock_blocking_connection.return_value = self.mock_connection
        self.mock_connection.close.side_effect = Exception("Close failed")
        middleware = MessageMiddlewareQueue(self.host, self.queue_name)
        
        with self.assertRaises(MessageMiddlewareCloseError):
            middleware.close()

    @patch('rabbitmq.middleware.pika.BlockingConnection')
    def test_callback_wrapper_success(self, mock_blocking_connection):
        """Test successful message processing in callback wrapper"""
        mock_blocking_connection.return_value = self.mock_connection
        middleware = MessageMiddlewareQueue(self.host, self.queue_name)
        
        mock_ch = Mock()
        mock_method = Mock()
        mock_method.delivery_tag = "test_tag"
        mock_properties = Mock()
        mock_body = b"test message"
        
        user_callback = Mock()
        wrapped_callback = middleware._wrap_callback(user_callback)
        
        wrapped_callback(mock_ch, mock_method, mock_properties, mock_body)
        
        user_callback.assert_called_once_with(mock_ch, mock_method, mock_properties, mock_body)
        mock_ch.basic_ack.assert_called_once_with(delivery_tag="test_tag")

    @patch('rabbitmq.middleware.pika.BlockingConnection')
    def test_callback_wrapper_error(self, mock_blocking_connection):
        """Test error handling in callback wrapper"""
        mock_blocking_connection.return_value = self.mock_connection
        middleware = MessageMiddlewareQueue(self.host, self.queue_name)
        
        mock_ch = Mock()
        mock_method = Mock()
        mock_method.delivery_tag = "test_tag"
        mock_properties = Mock()
        mock_body = b"test message"
        
        user_callback = Mock()
        user_callback.side_effect = Exception("Callback failed")
        wrapped_callback = middleware._wrap_callback(user_callback)
        
        wrapped_callback(mock_ch, mock_method, mock_properties, mock_body)
        
        user_callback.assert_called_once_with(mock_ch, mock_method, mock_properties, mock_body)
        mock_ch.basic_nack.assert_called_once_with(delivery_tag="test_tag", requeue=True)


class TestMessageMiddlewareExchange(unittest.TestCase):
    """Test suite for MessageMiddlewareExchange class"""

    def setUp(self):
        """Set up test fixtures before each test method."""
        self.host = "localhost"
        self.exchange_name = "test_exchange"
        self.route_keys = ["key1", "key2"]
        self.mock_connection = Mock()
        self.mock_channel = Mock()
        self.mock_connection.channel.return_value = self.mock_channel

    @patch('rabbitmq.middleware.pika.BlockingConnection')
    def test_init_success_with_list_keys(self, mock_blocking_connection):
        """Test successful initialization with list of routing keys"""
        mock_blocking_connection.return_value = self.mock_connection
        
        middleware = MessageMiddlewareExchange(self.host, self.exchange_name, self.route_keys)
        
        self.assertEqual(middleware.host, self.host)
        self.assertEqual(middleware.exchange_name, self.exchange_name)
        self.assertEqual(middleware.route_keys, self.route_keys)
        self.assertEqual(middleware.connection, self.mock_connection)
        self.assertEqual(middleware.channel, self.mock_channel)
        
        # Verify connection setup
        mock_blocking_connection.assert_called_once()
        self.mock_channel.exchange_declare.assert_called_once_with(
            exchange=self.exchange_name, 
            exchange_type='topic', 
            durable=True
        )

    @patch('rabbitmq.middleware.pika.BlockingConnection')
    def test_init_success_with_single_key(self, mock_blocking_connection):
        """Test successful initialization with single routing key"""
        mock_blocking_connection.return_value = self.mock_connection
        single_key = "single_key"
        
        middleware = MessageMiddlewareExchange(self.host, self.exchange_name, single_key)
        
        self.assertEqual(middleware.route_keys, [single_key])

    @patch('rabbitmq.middleware.pika.BlockingConnection')
    def test_init_connection_error(self, mock_blocking_connection):
        """Test initialization failure due to connection error"""
        mock_blocking_connection.side_effect = pika.exceptions.AMQPConnectionError("Connection failed")
        
        with self.assertRaises(MessageMiddlewareDisconnectedError):
            MessageMiddlewareExchange(self.host, self.exchange_name, self.route_keys)

    @patch('rabbitmq.middleware.pika.BlockingConnection')
    def test_send_message_with_routing_key(self, mock_blocking_connection):
        """Test successful message sending with specific routing key"""
        mock_blocking_connection.return_value = self.mock_connection
        middleware = MessageMiddlewareExchange(self.host, self.exchange_name, self.route_keys)
        
        test_message = "test message"
        routing_key = "custom_key"
        middleware.send(test_message, routing_key)
        
        self.mock_channel.basic_publish.assert_called_once_with(
            exchange=self.exchange_name,
            routing_key=routing_key,
            body=test_message,
            properties=unittest.mock.ANY
        )

    @patch('rabbitmq.middleware.pika.BlockingConnection')
    def test_send_message_default_routing_key(self, mock_blocking_connection):
        """Test successful message sending with default routing key"""
        mock_blocking_connection.return_value = self.mock_connection
        middleware = MessageMiddlewareExchange(self.host, self.exchange_name, self.route_keys)
        
        test_message = "test message"
        middleware.send(test_message)
        
        self.mock_channel.basic_publish.assert_called_once_with(
            exchange=self.exchange_name,
            routing_key=self.route_keys[0],  # Should use first routing key as default
            body=test_message,
            properties=unittest.mock.ANY
        )

    @patch('rabbitmq.middleware.pika.BlockingConnection')
    def test_send_message_no_connection(self, mock_blocking_connection):
        """Test sending message when there's no active connection"""
        mock_blocking_connection.return_value = self.mock_connection
        middleware = MessageMiddlewareExchange(self.host, self.exchange_name, self.route_keys)
        middleware.channel = None
        
        with self.assertRaises(MessageMiddlewareDisconnectedError):
            middleware.send("test message")

    @patch('rabbitmq.middleware.pika.BlockingConnection')
    def test_send_message_publish_error(self, mock_blocking_connection):
        """Test sending message when publish fails"""
        mock_blocking_connection.return_value = self.mock_connection
        self.mock_channel.basic_publish.side_effect = Exception("Publish failed")
        middleware = MessageMiddlewareExchange(self.host, self.exchange_name, self.route_keys)
        
        with self.assertRaises(MessageMiddlewareMessageError):
            middleware.send("test message")

    @patch('rabbitmq.middleware.pika.BlockingConnection')
    def test_start_consuming_success(self, mock_blocking_connection):
        """Test successful start of message consumption"""
        mock_blocking_connection.return_value = self.mock_connection
        
        # Mock queue declaration result
        mock_result = Mock()
        mock_result.method.queue = "temp_queue_123"
        self.mock_channel.queue_declare.return_value = mock_result
        
        middleware = MessageMiddlewareExchange(self.host, self.exchange_name, self.route_keys)
        
        callback = Mock()
        middleware.start_consuming(callback)
        
        # Verify queue binding for each routing key
        expected_calls = [
            call(exchange=self.exchange_name, queue="temp_queue_123", routing_key=key)
            for key in self.route_keys
        ]
        self.mock_channel.queue_bind.assert_has_calls(expected_calls)
        
        self.mock_channel.basic_qos.assert_called_once_with(prefetch_count=1)
        self.mock_channel.basic_consume.assert_called_once()
        self.mock_channel.start_consuming.assert_called_once()

    @patch('rabbitmq.middleware.pika.BlockingConnection')
    def test_start_consuming_no_connection(self, mock_blocking_connection):
        """Test start consuming when there's no active connection"""
        mock_blocking_connection.return_value = self.mock_connection
        middleware = MessageMiddlewareExchange(self.host, self.exchange_name, self.route_keys)
        middleware.channel = None
        
        callback = Mock()
        with self.assertRaises(MessageMiddlewareDisconnectedError):
            middleware.start_consuming(callback)

    @patch('rabbitmq.middleware.pika.BlockingConnection')
    def test_start_consuming_connection_error(self, mock_blocking_connection):
        """Test start consuming when connection is lost"""
        mock_blocking_connection.return_value = self.mock_connection
        self.mock_channel.start_consuming.side_effect = pika.exceptions.AMQPConnectionError("Connection lost")
        
        # Mock queue declaration result
        mock_result = Mock()
        mock_result.method.queue = "temp_queue_123"
        self.mock_channel.queue_declare.return_value = mock_result
        
        middleware = MessageMiddlewareExchange(self.host, self.exchange_name, self.route_keys)
        
        callback = Mock()
        with self.assertRaises(MessageMiddlewareDisconnectedError):
            middleware.start_consuming(callback)

    @patch('rabbitmq.middleware.pika.BlockingConnection')
    def test_stop_consuming_success(self, mock_blocking_connection):
        """Test successful stop of message consumption"""
        mock_blocking_connection.return_value = self.mock_connection
        middleware = MessageMiddlewareExchange(self.host, self.exchange_name, self.route_keys)
        
        middleware.stop_consuming()
        
        self.mock_channel.stop_consuming.assert_called_once()

    @patch('rabbitmq.middleware.pika.BlockingConnection')
    def test_stop_consuming_error(self, mock_blocking_connection):
        """Test stop consuming when an error occurs"""
        mock_blocking_connection.return_value = self.mock_connection
        self.mock_channel.stop_consuming.side_effect = Exception("Stop failed")
        middleware = MessageMiddlewareExchange(self.host, self.exchange_name, self.route_keys)
        
        with self.assertRaises(MessageMiddlewareMessageError):
            middleware.stop_consuming()

    @patch('rabbitmq.middleware.pika.BlockingConnection')
    def test_delete_exchange_success(self, mock_blocking_connection):
        """Test successful exchange deletion"""
        mock_blocking_connection.return_value = self.mock_connection
        middleware = MessageMiddlewareExchange(self.host, self.exchange_name, self.route_keys)
        
        middleware.delete()
        
        self.mock_channel.exchange_delete.assert_called_once_with(exchange=self.exchange_name)

    @patch('rabbitmq.middleware.pika.BlockingConnection')
    def test_delete_exchange_error(self, mock_blocking_connection):
        """Test exchange deletion when an error occurs"""
        mock_blocking_connection.return_value = self.mock_connection
        self.mock_channel.exchange_delete.side_effect = Exception("Delete failed")
        middleware = MessageMiddlewareExchange(self.host, self.exchange_name, self.route_keys)
        
        with self.assertRaises(MessageMiddlewareDeleteError):
            middleware.delete()

    @patch('rabbitmq.middleware.pika.BlockingConnection')
    def test_close_connection_success(self, mock_blocking_connection):
        """Test successful connection closure"""
        mock_blocking_connection.return_value = self.mock_connection
        middleware = MessageMiddlewareExchange(self.host, self.exchange_name, self.route_keys)
        
        middleware.close()
        
        self.mock_connection.close.assert_called_once()

    @patch('rabbitmq.middleware.pika.BlockingConnection')
    def test_close_connection_error(self, mock_blocking_connection):
        """Test connection closure when an error occurs"""
        mock_blocking_connection.return_value = self.mock_connection
        self.mock_connection.close.side_effect = Exception("Close failed")
        middleware = MessageMiddlewareExchange(self.host, self.exchange_name, self.route_keys)
        
        with self.assertRaises(MessageMiddlewareCloseError):
            middleware.close()

    @patch('rabbitmq.middleware.pika.BlockingConnection')
    def test_callback_wrapper_success(self, mock_blocking_connection):
        """Test successful message processing in callback wrapper"""
        mock_blocking_connection.return_value = self.mock_connection
        middleware = MessageMiddlewareExchange(self.host, self.exchange_name, self.route_keys)
        
        mock_ch = Mock()
        mock_method = Mock()
        mock_method.delivery_tag = "test_tag"
        mock_properties = Mock()
        mock_body = b"test message"
        
        user_callback = Mock()
        wrapped_callback = middleware._wrap_callback(user_callback)
        
        wrapped_callback(mock_ch, mock_method, mock_properties, mock_body)
        
        user_callback.assert_called_once_with(mock_ch, mock_method, mock_properties, mock_body)
        mock_ch.basic_ack.assert_called_once_with(delivery_tag="test_tag")

    @patch('rabbitmq.middleware.pika.BlockingConnection')
    def test_callback_wrapper_error(self, mock_blocking_connection):
        """Test error handling in callback wrapper"""
        mock_blocking_connection.return_value = self.mock_connection
        middleware = MessageMiddlewareExchange(self.host, self.exchange_name, self.route_keys)
        
        mock_ch = Mock()
        mock_method = Mock()
        mock_method.delivery_tag = "test_tag"
        mock_properties = Mock()
        mock_body = b"test message"
        
        user_callback = Mock()
        user_callback.side_effect = Exception("Callback failed")
        wrapped_callback = middleware._wrap_callback(user_callback)
        
        wrapped_callback(mock_ch, mock_method, mock_properties, mock_body)
        
        user_callback.assert_called_once_with(mock_ch, mock_method, mock_properties, mock_body)
        mock_ch.basic_nack.assert_called_once_with(delivery_tag="test_tag", requeue=True)


if __name__ == '__main__':
    unittest.main()

import unittest
import threading
import time
import queue
import sys
import os
from unittest.mock import patch

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

try:
    from rabbitmq.middleware import (
        MessageMiddlewareQueue,
        MessageMiddlewareExchange,
    )
except ImportError:
    middleware_path = os.path.join(project_root, 'rabbitmq')
    sys.path.insert(0, middleware_path)
    from middleware import (
        MessageMiddlewareQueue,
        MessageMiddlewareExchange,
    )


class TestMessageMiddlewareIntegration(unittest.TestCase):
    """Integration tests for MessageMiddleware classes testing 1:1 and 1:N communication patterns"""

    def setUp(self):
        """Set up test fixtures before each test method."""
        self.host = "localhost"
        self.timeout = 5  # seconds
        self.received_messages = queue.Queue()
        
    def tearDown(self):
        """Clean up after each test method."""
        while not self.received_messages.empty():
            try:
                self.received_messages.get_nowait()
            except queue.Empty:
                break

    def message_callback_factory(self, consumer_id):
        """Factory to create message callbacks that store messages with consumer ID"""
        def callback(ch, method, properties, body):
            message = {
                'consumer_id': consumer_id,
                'body': body.decode('utf-8'),
                'routing_key': method.routing_key if hasattr(method, 'routing_key') else None
            }
            self.received_messages.put(message)
        return callback

    @patch('rabbitmq.middleware.pika.BlockingConnection')
    def test_working_queue_1_to_1_communication(self, mock_blocking_connection):
        """Test 1:1 communication using working queue pattern"""
        # Setup mocks
        mock_connection = unittest.mock.Mock()
        mock_channel = unittest.mock.Mock()
        mock_connection.channel.return_value = mock_channel
        mock_blocking_connection.return_value = mock_connection
        
        # Simulate message consumption by calling callback directly
        def mock_basic_consume(queue, on_message_callback, auto_ack):
            # Store the callback for later simulation
            self.stored_callback = on_message_callback
            
        mock_channel.basic_consume.side_effect = mock_basic_consume
        
        # Create producer and consumer
        producer = MessageMiddlewareQueue(self.host, "test_queue_1to1")
        consumer = MessageMiddlewareQueue(self.host, "test_queue_1to1")
        
        # Start consumer in a separate thread
        consumer_thread = threading.Thread(
            target=consumer.start_consuming,
            args=(self.message_callback_factory("consumer1"),)
        )
        consumer_thread.daemon = True
        consumer_thread.start()
        
        # Give consumer time to start
        time.sleep(0.1)
        
        # Send a message
        test_message = "Hello from producer to single consumer"
        producer.send(test_message)
        
        # Simulate message delivery by calling the stored callback
        if hasattr(self, 'stored_callback'):
            mock_method = unittest.mock.Mock()
            mock_method.delivery_tag = "tag1"
            mock_properties = unittest.mock.Mock()
            self.stored_callback(mock_channel, mock_method, mock_properties, test_message.encode('utf-8'))
        
        # Wait for message to be processed
        time.sleep(0.1)
        
        # Verify message was received
        self.assertFalse(self.received_messages.empty())
        received = self.received_messages.get(timeout=1)
        
        self.assertEqual(received['consumer_id'], "consumer1")
        self.assertEqual(received['body'], test_message)
        
        # Cleanup
        consumer.stop_consuming()
        producer.close()
        consumer.close()

    @patch('rabbitmq.middleware.pika.BlockingConnection')
    def test_working_queue_1_to_n_communication(self, mock_blocking_connection):
        """Test 1:N communication using working queue pattern"""
        # Setup mocks
        mock_connection = unittest.mock.Mock()
        mock_channel = unittest.mock.Mock()
        mock_connection.channel.return_value = mock_channel
        mock_blocking_connection.return_value = mock_connection
        
        stored_callbacks = []
        
        def mock_basic_consume(queue, on_message_callback, auto_ack):
            stored_callbacks.append(on_message_callback)
            
        mock_channel.basic_consume.side_effect = mock_basic_consume
        
        # Create producer and multiple consumers
        producer = MessageMiddlewareQueue(self.host, "test_queue_1toN")
        consumers = []
        consumer_threads = []
        
        num_consumers = 3
        for i in range(num_consumers):
            consumer = MessageMiddlewareQueue(self.host, "test_queue_1toN")
            consumers.append(consumer)
            
            # Start consumer in separate thread
            thread = threading.Thread(
                target=consumer.start_consuming,
                args=(self.message_callback_factory(f"consumer{i+1}"),)
            )
            thread.daemon = True
            consumer_threads.append(thread)
            thread.start()
        
        # Give consumers time to start
        time.sleep(0.1)
        
        # Send multiple messages
        messages = [f"Message {i+1}" for i in range(5)]
        for msg in messages:
            producer.send(msg)
            
            # Simulate round-robin distribution to consumers
            if stored_callbacks:
                callback_index = len([m for m in messages if messages.index(m) <= messages.index(msg)]) % len(stored_callbacks)
                callback = stored_callbacks[callback_index]
                
                mock_method = unittest.mock.Mock()
                mock_method.delivery_tag = f"tag{messages.index(msg)+1}"
                mock_properties = unittest.mock.Mock()
                callback(mock_channel, mock_method, mock_properties, msg.encode('utf-8'))
        
        # Wait for messages to be processed
        time.sleep(0.2)
        
        # Verify all messages were received and distributed
        received_messages = []
        while not self.received_messages.empty():
            try:
                received_messages.append(self.received_messages.get_nowait())
            except queue.Empty:
                break
        
        self.assertEqual(len(received_messages), len(messages))
        
        # Verify messages were distributed among consumers
        consumer_counts = {}
        for msg in received_messages:
            consumer_id = msg['consumer_id']
            consumer_counts[consumer_id] = consumer_counts.get(consumer_id, 0) + 1
        
        # In a working queue, each message should go to only one consumer
        total_received = sum(consumer_counts.values())
        self.assertEqual(total_received, len(messages))
        
        # Cleanup
        for consumer in consumers:
            consumer.stop_consuming()
            consumer.close()
        producer.close()

    @patch('rabbitmq.middleware.pika.BlockingConnection')
    def test_exchange_1_to_1_communication(self, mock_blocking_connection):
        """Test 1:1 communication using exchange pattern"""
        # Setup mocks
        mock_connection = unittest.mock.Mock()
        mock_channel = unittest.mock.Mock()
        mock_connection.channel.return_value = mock_channel
        mock_blocking_connection.return_value = mock_connection
        
        # Mock queue declaration for consumer
        mock_result = unittest.mock.Mock()
        mock_result.method.queue = "temp_queue_123"
        mock_channel.queue_declare.return_value = mock_result
        
        def mock_basic_consume(queue, on_message_callback, auto_ack):
            self.stored_callback = on_message_callback
            
        mock_channel.basic_consume.side_effect = mock_basic_consume
        
        routing_key = "test.routing.key"
        
        # Create producer and consumer
        producer = MessageMiddlewareExchange(self.host, "test_exchange_1to1", routing_key)
        consumer = MessageMiddlewareExchange(self.host, "test_exchange_1to1", routing_key)
        
        # Start consumer
        consumer_thread = threading.Thread(
            target=consumer.start_consuming,
            args=(self.message_callback_factory("consumer1"),)
        )
        consumer_thread.daemon = True
        consumer_thread.start()
        
        time.sleep(0.1)
        
        # Send message
        test_message = "Hello from exchange producer"
        producer.send(test_message, routing_key)
        
        # Simulate message delivery
        if hasattr(self, 'stored_callback'):
            mock_method = unittest.mock.Mock()
            mock_method.delivery_tag = "tag1"
            mock_method.routing_key = routing_key
            mock_properties = unittest.mock.Mock()
            self.stored_callback(mock_channel, mock_method, mock_properties, test_message.encode('utf-8'))
        
        time.sleep(0.1)
        
        # Verify message was received
        self.assertFalse(self.received_messages.empty())
        received = self.received_messages.get(timeout=1)
        
        self.assertEqual(received['consumer_id'], "consumer1")
        self.assertEqual(received['body'], test_message)
        self.assertEqual(received['routing_key'], routing_key)
        
        # Cleanup
        consumer.stop_consuming()
        producer.close()
        consumer.close()

    @patch('rabbitmq.middleware.pika.BlockingConnection')
    def test_exchange_1_to_n_communication(self, mock_blocking_connection):
        """Test 1:N communication using exchange pattern"""
        # Setup mocks
        mock_connection = unittest.mock.Mock()
        mock_channel = unittest.mock.Mock()
        mock_connection.channel.return_value = mock_channel
        mock_blocking_connection.return_value = mock_connection
        
        # Mock different queue declarations for each consumer
        queue_counter = [0]
        def mock_queue_declare(queue='', exclusive=True):
            queue_counter[0] += 1
            mock_result = unittest.mock.Mock()
            mock_result.method.queue = f"temp_queue_{queue_counter[0]}"
            return mock_result
            
        mock_channel.queue_declare.side_effect = mock_queue_declare
        
        stored_callbacks = []
        
        def mock_basic_consume(queue, on_message_callback, auto_ack):
            stored_callbacks.append(on_message_callback)
            
        mock_channel.basic_consume.side_effect = mock_basic_consume
        
        routing_key = "test.broadcast.key"
        
        # Create producer and multiple consumers
        producer = MessageMiddlewareExchange(self.host, "test_exchange_1toN", routing_key)
        consumers = []
        consumer_threads = []
        
        num_consumers = 3
        for i in range(num_consumers):
            consumer = MessageMiddlewareExchange(self.host, "test_exchange_1toN", routing_key)
            consumers.append(consumer)
            
            thread = threading.Thread(
                target=consumer.start_consuming,
                args=(self.message_callback_factory(f"consumer{i+1}"),)
            )
            thread.daemon = True
            consumer_threads.append(thread)
            thread.start()
        
        time.sleep(0.1)
        
        # Send a broadcast message
        test_message = "Broadcast message to all consumers"
        producer.send(test_message, routing_key)
        
        # Simulate message delivery to all consumers (pub/sub pattern)
        for i, callback in enumerate(stored_callbacks):
            mock_method = unittest.mock.Mock()
            mock_method.delivery_tag = f"tag{i+1}"
            mock_method.routing_key = routing_key
            mock_properties = unittest.mock.Mock()
            callback(mock_channel, mock_method, mock_properties, test_message.encode('utf-8'))
        
        time.sleep(0.2)
        
        # Verify all consumers received the message
        received_messages = []
        while not self.received_messages.empty():
            try:
                received_messages.append(self.received_messages.get_nowait())
            except queue.Empty:
                break
        
        # In pub/sub, all consumers should receive the same message
        self.assertEqual(len(received_messages), num_consumers)
        
        consumer_ids = set()
        for msg in received_messages:
            self.assertEqual(msg['body'], test_message)
            self.assertEqual(msg['routing_key'], routing_key)
            consumer_ids.add(msg['consumer_id'])
        
        # Verify all consumers received the message
        expected_consumer_ids = {f"consumer{i+1}" for i in range(num_consumers)}
        self.assertEqual(consumer_ids, expected_consumer_ids)
        
        # Cleanup
        for consumer in consumers:
            consumer.stop_consuming()
            consumer.close()
        producer.close()

    @patch('rabbitmq.middleware.pika.BlockingConnection')
    def test_exchange_multiple_routing_keys(self, mock_blocking_connection):
        """Test exchange with multiple routing keys"""
        # Setup mocks
        mock_connection = unittest.mock.Mock()
        mock_channel = unittest.mock.Mock()
        mock_connection.channel.return_value = mock_channel
        mock_blocking_connection.return_value = mock_connection
        
        mock_result = unittest.mock.Mock()
        mock_result.method.queue = "temp_queue_multi"
        mock_channel.queue_declare.return_value = mock_result
        
        def mock_basic_consume(queue, on_message_callback, auto_ack):
            self.stored_callback = on_message_callback
            
        mock_channel.basic_consume.side_effect = mock_basic_consume
        
        routing_keys = ["key1", "key2", "key3"]
        
        # Create producer and consumer with multiple routing keys
        producer = MessageMiddlewareExchange(self.host, "test_exchange_multi", routing_keys)
        consumer = MessageMiddlewareExchange(self.host, "test_exchange_multi", routing_keys)
        
        # Start consumer
        consumer_thread = threading.Thread(
            target=consumer.start_consuming,
            args=(self.message_callback_factory("multi_consumer"),)
        )
        consumer_thread.daemon = True
        consumer_thread.start()
        
        time.sleep(0.1)
        
        # Send messages with different routing keys
        messages = [
            ("Message for key1", "key1"),
            ("Message for key2", "key2"),
            ("Message for key3", "key3")
        ]
        
        for message, key in messages:
            producer.send(message, key)
            
            # Simulate message delivery
            if hasattr(self, 'stored_callback'):
                mock_method = unittest.mock.Mock()
                mock_method.delivery_tag = f"tag_{key}"
                mock_method.routing_key = key
                mock_properties = unittest.mock.Mock()
                self.stored_callback(mock_channel, mock_method, mock_properties, message.encode('utf-8'))
        
        time.sleep(0.2)
        
        # Verify all messages were received
        received_messages = []
        while not self.received_messages.empty():
            try:
                received_messages.append(self.received_messages.get_nowait())
            except queue.Empty:
                break
        
        self.assertEqual(len(received_messages), len(messages))
        
        # Verify each message was received with correct routing key
        for msg in received_messages:
            self.assertEqual(msg['consumer_id'], "multi_consumer")
            self.assertIn(msg['routing_key'], routing_keys)
            
        # Verify all routing keys were used
        received_keys = {msg['routing_key'] for msg in received_messages}
        self.assertEqual(received_keys, set(routing_keys))
        
        # Cleanup
        consumer.stop_consuming()
        producer.close()
        consumer.close()


if __name__ == '__main__':
    unittest.main()

module devices;

// Get the base objects
import zmq;
import dzmq;

import std.stdio : writef;

class DZMQDevice {
	immutable enum Type {
	    STREAMER     = ZMQ_STREAMER,
	    FORWARDER    = ZMQ_FORWARDER,
	    QUEUE        = ZMQ_QUEUE,
	}
	private {
		Socket front, back;
	}
	this(Socket f, Socket b, Type type) {
		this.front = f;
		this.back = b;
		if(zmq_device(type, f.raw, b.raw) != 0) {
			throw new ZMQError();
		}
	}
}

class QueueDevice : DZMQDevice {
	this(Socket f, Socket b)
	in {
		assert(f.type == Socket.Type.ROUTER);
		assert(b.type == Socket.Type.DEALER);
	}body {
		super(f,b, Type.QUEUE);
	}
}

class ForwarderDevice : DZMQDevice {
	this(Socket f, Socket b)
	in {
		assert(f.type == Socket.Type.SUB);
		assert(b.type == Socket.Type.PUB);
	}body {
		super(f,b, Type.QUEUE);
	}
}

class StreamerDevice : DZMQDevice {
	this(Socket f, Socket b)
	in {
		assert(f.type == Socket.Type.PULL);
		assert(b.type == Socket.Type.PUSH);
	}body {
		super(f,b, Type.QUEUE);
	}
}

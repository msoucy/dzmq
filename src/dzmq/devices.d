/** @file devices.d
@brief D ZeroMQ device classes
@authors Matthew Soucy <msoucy@csh.rit.edu>
@date May 9, 2012
@todo Look into using templates with Device.Type instead of separate classes
*/
///D ZeroMQ device classes
module devices;

/// @cond NoDoc
// Get the base objects
private import ZeroMQ.zmq;
import dzmq;

import std.stdio : writef;
/// @endcond

/**
@brief Interface for all devices
A device connects two related sockets, and is typically used for transferring data along a chain.
*/
interface Device {
	/// Type of device
	immutable enum Type {
		/// Handles Push/Pull connections
	    STREAMER     = ZMQ_STREAMER,
	    /// Handles Pub/Sub connections
	    FORWARDER    = ZMQ_FORWARDER,
	    /// Handles Router/Dealer connections
	    QUEUE        = ZMQ_QUEUE,
	    /// User-defined connection, not built in to 0MQ
	    CUSTOM,
	}
	/**
	@brief Run a ZMQ Device
	
	This action will block until the device's context is destroyed or the function terminates.
	*/
	void run();
}

/// Wrapper for all of the builtin device types
abstract class DZMQDevice : Device {
	private {
		Socket front, back;
		Type type;
	}
	/**
	@param front Frontend socket
	@param back Backend socket
	@param type Type of the device
	*/
	this(Socket front, Socket back, Type type=Type.CUSTOM) {
		this.front = front;
		this.back = back;
		this.type = type;
	}
	void run() {
		if(this.type != Type.CUSTOM) {
			static if(ZMQ_VERSION_MAJOR == 2) {
				// 0MQ version 2 supports devices, but they're gone in 3
				if(zmq_device(type, this.front.raw, this.back.raw) != 0) {
					throw new ZMQError();
				}
			} else static if(ZMQ_VERSION_MAJOR == 3) {
				// We'll have to support this at some point...
			} else {
				static assert(0,"Unknown 0MQ version");
			}
		}
	}
}

/// Wrapper for a Streamer device
class StreamerDevice : DZMQDevice {
	/**
	Create a Streamer device to pull data from one socket and push it out another
	@param front A PULL-type socket
	@param back A PUSH-type socket
	*/
	this(Socket front, Socket back)
	in {
		assert(front.type == Socket.Type.PULL);
		assert(back.type == Socket.Type.PUSH);
	} body {
		super(front, back, Type.STREAMER);
	}
}

/// Wrapper for a Forwarder device
class ForwarderDevice : DZMQDevice {
	/**
	Create a Forwarder device to subscribe to certain topics and publish them on another socket
	@param front A SUB-type socket
	@param back A PUB-type socket
	*/
	this(Socket front, Socket back)
	in {
		assert(front.type == Socket.Type.SUB);
		assert(back.type == Socket.Type.PUB);
	} body {
		super(front, back, Type.FORWARDER);
	}
}

/// Wrapper for a Queue device
class QueueDevice : DZMQDevice {
	/**
	Create a Queue device.
	
	This interfaces with REQ-REP sockets
	@param front A SUB-type socket
	@param back A PUB-type socket
	*/
	this(Socket front, Socket back)
	in {
		assert(front.type == Socket.Type.ROUTER);
		assert(back.type == Socket.Type.DEALER);
	} body {
		super(front, back, Type.QUEUE);
	}
}

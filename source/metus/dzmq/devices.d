/*******************************************************************************
 * D ZeroMQ device classes
 *
 * Authors: Matthew Soucy, msoucy@csh.rit.edu
 * Date: May 9, 2012
 * Todo: Look into using templates with Device.Type instead of separate classes
 */
module metus.dzmq.devices;

// Get the base objects
import metus.dzmq.dzmq;

/*******************************************************************************
 * Interface for all devices
 *
 * A device connects two related sockets,
 * and is typically used for transferring data along a chain.
 */
interface Device {
	/// Type of device
	immutable enum Type {
		/// Handles Push/Pull connections
		STREAMER     = zmq.ZMQ_STREAMER,
		/// Handles Pub/Sub connections
		FORWARDER    = zmq.ZMQ_FORWARDER,
		/// Handles Router/Dealer connections
		QUEUE        = zmq.ZMQ_QUEUE,
		/// User-defined connection, not built in to D0MQ
		CUSTOM,
	}
	/**
	 * Run a ZMQ Device
	 *
	 * This action will block until the device's context is destroyed or the function terminates.
	 */
	void run();
}

/// Wrapper for all of the builtin device types
private abstract class DZMQDevice : Device {
	private {
		Socket front, back;
		Type type;
	}
	/**
	 * Creates a generic Device based on a builtin ZeroMQ device type
	 *
	 * Params:
	 *			front	=	The frontend socket
	 *			back	=	The backend socket
	 *			type	=	The type of the device
	 */
	@safe nothrow this(Socket front, Socket back, Type type=Type.CUSTOM) {
		this.front = front;
		this.back = back;
		this.type = type;
	}
	/**
	 * Perform the main Device operation
	 *
	 * Starts the Device to transfer data
	 */
	final void run() {
		if(this.type != Type.CUSTOM) {
			static if(zmq.ZMQ_VERSION_MAJOR == 2) {
				// 0MQ version 2 supports devices, but they're gone in 3
				if(zmq.zmq_device(type, this.front.raw, this.back.raw) != 0) {
					throw new ZMQException();
				}
			} else static if(zmq.ZMQ_VERSION_MAJOR == 3) {
				// We'll have to support this at some point...
				static assert(0, "Unsupported 0MQ version");
			} else {
				static assert(0, "Unknown 0MQ version");
			}
		}
	}
}

/// Wrapper for a Streamer device
final class StreamerDevice : DZMQDevice {
	/**
	 * Create a Streamer device to pull data from one socket and push it out another
	 *
	 * Params:
	 *			front	=	A PULL-type socket
	 *			back	=	A PUSH-type socket
	 */
	@safe nothrow this(Socket front, Socket back)
	in {
		assert(front.type == Socket.Type.PULL);
		assert(back.type == Socket.Type.PUSH);
	}
	body {
		super(front, back, Type.STREAMER);
	}
}

/// Wrapper for a Forwarder device
final class ForwarderDevice : DZMQDevice {
	/**
	 * Create a Forwarder device to subscribe to certain topics and publish them on another socket
	 *
	 * Params:
	 *			front	=	A SUB-type socket
	 *			back	=	A PUB-type socket
	 */
	@safe nothrow this(Socket front, Socket back)
	in {
		assert(front.type == Socket.Type.SUB);
		assert(back.type == Socket.Type.PUB);
	}
	body {
		super(front, back, Type.FORWARDER);
	}
}

/// Wrapper for a Queue device
final class QueueDevice : DZMQDevice {
	/**
	 * Create a Queue device.
	 *
	 * This interfaces with REQ-REP sockets
	 *
	 * Params:
	 *			front	=	A ROUTER-type socket
	 *			back	=	A DEALER-type socket
	 */
	@safe nothrow this(Socket front, Socket back)
	in {
		assert(front.type == Socket.Type.ROUTER);
		assert(back.type == Socket.Type.DEALER);
	}
	body {
		super(front, back, Type.QUEUE);
	}
}

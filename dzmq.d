/**
 * @file dzmq.d
 * @brief D ZeroMQ class wrappers
 * @author Matthew Soucy <msoucy@csh.rit.edu>
 * @date May 9, 2012
 * @version 0.0.1
 */
///D ZeroMQ class wrappers
module metus.dzmq.dzmq;

/// @cond NoDoc
package import zmq = deimos.zmq.zmq;

import core.stdc.errno;
import std.string : toStringz, format, strlen;
import std.algorithm : canFind;
import std.stdio;
/// @endcond

/**
 * Get the 0MQ version
 *
 * Stores the 0MQ version in the passed-in parameter references
 *
 * @param[out] major The major version
 * @param[out] minor The minor version
 * @param[out] patch The patch version
 */
void zmq_version(ref int major, ref int minor, ref int patch) {
	return zmq.zmq_version(&major, &minor, &patch);
}

/**
 * Get the 0MQ version
 *
 * @return The 0MQ version as a string
 */
string zmq_version() {
	int major, minor, patch;
	zmq_version(major, minor, patch);
	return "%s.%s.%s".format(major, minor, patch);
}

/**
 * ZeroMQ context manager
 *
 * Manages the context for all sockets within a thread
 */
class Context {
	private {
		void* context;
	}
	/**
	 * Create a 0MQ context to manage all sockets within a thread
	 *
	 * @param io_threads Number of threads to use for the context
	 */
	this(int io_threads=0) {
		this.context = zmq.zmq_init(io_threads);
	}
	~this() {
		zmq.zmq_term(this.context);
		context = null;
	}
	/**
	 * Get a raw pointer to the context
	 *
	 * @return Pointer to the block of data representing the context
	 */
	void* raw() @safe @property pure nothrow {return context;}
}

/**
 * @brief ZeroMQ socket class
 * Wraps a ZeroMQ socket and handles connections and data transfer
 *
 * @todo Add support for zmq.ZMQ_FD, to get the file descriptor (if valid in D)
 * @todo Add support for zmq.ZMQ_EVENTS - this requires zmq.ZMQ_POLLIN and zmq.ZMQ_POLLOUT to be wrapped
 */
class Socket {
	/// Socket types
	/**
	 * Types for a socket
	 *
	 * Defined to be the same as in ZMQ
	 */
	public immutable enum Type {
		/// Pair
		PAIR        = zmq.ZMQ_PAIR,
		/// Publisher
		PUB         = zmq.ZMQ_PUB,
		/// Subscriber
		SUB         = zmq.ZMQ_SUB,
		/// Request
		REQ         = zmq.ZMQ_REQ,
		/// Reply
		REP         = zmq.ZMQ_REP,
		/// Dealer
		DEALER      = zmq.ZMQ_DEALER,
		/// Router
		ROUTER      = zmq.ZMQ_ROUTER,
		/// Pulling
		PULL        = zmq.ZMQ_PULL,
		/// Pushing
		PUSH        = zmq.ZMQ_PUSH,
		/// Extended publisher
		XPUB        = zmq.ZMQ_XPUB,
		/// Extended subscriber
		XSUB        = zmq.ZMQ_XSUB,
	}
	/**
	 * Message sending flags
	 */
	public immutable enum Flags {
		/// Send/receive nonblocking
		NOBLOCK = zmq.ZMQ_NOBLOCK,
		/// This is the first part of a sent message
		SNDMORE = zmq.ZMQ_SNDMORE,
	}

	private {
		void* socket;
		Type _type;

		/**
		 * Packs a string into a block of data
		 *
		 * @param destination Pointer to a valid block of data to write to
		 * @param data The string data to write
		 */
		void msg_pack(void* destination, string data) {
			size_t i=0;
			while(i < data.length){
				*cast(char*)(destination++) = data[i++];
			}
		}
		/**
		 * Unpacks a string from a zmq.zmq_msg_t
		 *
		 * @param msg The message to unpack
		 * @returns The stored string
		 */
		string msg_unpack(zmq.zmq_msg_t msg) {
			size_t i=zmq.zmq_msg_size(&msg);
			string ret=cast(string)(zmq.zmq_msg_data(&msg)[0..i]).idup;
			return ret;
		}

		mixin template SocketOption(TYPE, string NAME, int VALUE) {
			/// Setter
			void SocketOption(TYPE value) @property {
				if(zmq.zmq_setsockopt(this.socket, VALUE, &value, TYPE.sizeof) == -1) {
					throw new ZMQException();
				}
			}
			/// Getter
			TYPE SocketOption() @property {
				TYPE ret;
				size_t size = TYPE.sizeof;
				if(zmq.zmq_getsockopt(this.socket, VALUE, &ret, &size) == -1) {
					throw new ZMQException();
				} else {
					return ret;
				}
			}
			mixin("alias SocketOption "~NAME~";");
		}
	}

	/**
	 * Creates and initializes a socket
	 *
	 * @param context The 0MQ context to use for the socket's creation
	 * @param type The type of the socket
	 */
	this(Context context, Type type) {
		socket = zmq.zmq_socket(context.raw, cast(int)type);
		this._type = type;
	}
	/**
	 * Cleans up after a socket
	 */
	~this() {
		zmq.zmq_close(this.socket);
	}

	/**
	 * Bind a socket to an address
	 *
	 * @param addr The address to bind to
	 */
	void bind(string addr) {
		if(zmq.zmq_bind (this.socket, addr.toStringz()) == -1) {
			throw new ZMQException();
		}
	}
	/**
	 * Connect a socket to an address
	 *
	 * @param endpoint The address to connect to
	 */
	void connect(string endpoint) {
		if(zmq.zmq_connect(this.socket, endpoint.toStringz()) == -1) {
			throw new ZMQException();
		}
	}

	/**
	 * Send a message
	 *
	 * @param msg Data to send
	 * @param flags Send flags
	 */
	void send(string msg, int flags=0) {
		zmq.zmq_msg_t zmsg;
		zmq.zmq_msg_init_size(&zmsg, msg.length);
		scope(exit) zmq.zmq_msg_close (&zmsg);
		msg_pack(zmq.zmq_msg_data (&zmsg), msg);
		if(zmq.zmq_send(this.socket, &zmsg, flags) == -1) {
			throw new ZMQException();
		}
	}
	/**
	 * Send a multipart message
	 *
	 * Each string is sent as a separate part of the message
	 *
	 * @param msg Data to send
	 * @param flags Send flags
	 */
	void send(string msg[], int flags=0) {
		for(size_t i=0; i+1 < msg.length; i++) {
			this.send(msg[i], flags|Flags.SNDMORE);
		}
		this.send(msg[$-1], flags);
	}

	/**
	 * Receive a message
	 *
	 * @param flags Receive flags
	 * @return A string storing the data received
	 */
	string recv(int flags=0) {
		zmq.zmq_msg_t zmsg;
		zmq.zmq_msg_init(&zmsg);
		scope(exit) zmq.zmq_msg_close(&zmsg);

		auto err = zmq.zmq_recv(this.socket, &zmsg, flags);
		string ret = msg_unpack(zmsg);
		if(flags&Flags.NOBLOCK && err == -1 && zmq.zmq_errno() == EAGAIN) {
			return null;
		} else if(err == -1) {
			throw new ZMQException();
		} else {
			return ret;
		}
	}
	/**
	 * Receive a message
	 *
	 * Each string is received as a separate part of the message
	 *
	 * @param flags Receive flags
	 * @return All data strings in the message
	 */
	string[] recv_multipart(int flags=0) {
		auto pack = this.recv(flags);
		if(flags&Flags.NOBLOCK && pack==null) {
			return null;
		}
		string[] parts = [pack];
		while(this.more) {
			parts ~= this.recv(flags);
		}
		return parts;
	}

	/**
	 * @name High water mark
	 *
	 * The number of messages that can build up in the socket's queue
	 *
	 * @see http://api.zeromq.org/2-1:zmq-setsockopt#toc3
	 */
	mixin SocketOption!(ulong, "hwm", zmq.ZMQ_HWM);

	/**
	 * @name Disk offload swap size
	 *
	 * The size (in bytes) of disk memory to store outstanding messages
	 *
	 * @see http://api.zeromq.org/2-1:zmq-setsockopt#toc4
	 */
	mixin SocketOption!(long, "swap", zmq.ZMQ_SWAP);

	/**
	 * @name I/O thread affinity
	 *
	 * Determines which threads to use for socket I/O
	 *
	 * @see http://api.zeromq.org/2-1:zmq-setsockopt#toc5
	 */
	mixin SocketOption!(ulong, "affinity", zmq.ZMQ_AFFINITY);

	/**
	 * @name Multicast data rate
	 *
	 * The maximum send or receive data rate for multicast transports
	 *
	 * @see http://api.zeromq.org/2-1:zmq-setsockopt#toc9
	 */
	mixin SocketOption!(long, "rate", zmq.ZMQ_RATE);

	/**
	 * @name Multicast recovery interval
	 *
	 * The maximum time in seconds that a receiver can be absent from a multicast
	 * group before unrecoverable data loss will occur.
	 *
	 * @see http://api.zeromq.org/2-1:zmq-setsockopt#toc10
	 * @see http://api.zeromq.org/2-1:zmq-setsockopt#toc11
	 */
	mixin SocketOption!(long, "rec_ivl", zmq.ZMQ_RECOVERY_IVL);
	mixin SocketOption!(long, "rec_ivl_msec", zmq.ZMQ_RECOVERY_IVL_MSEC);

	/**
	 * @name Multicast loopback
	 *
	 * Enables or disables the ability to receive transports from itself via loopback
	 *
	 * @see http://api.zeromq.org/2-1:zmq-setsockopt#toc12
	 */
	mixin SocketOption!(long, "mcast", zmq.ZMQ_MCAST_LOOP);

	/**
	 * @name Send buffer
	 *
	 * The underlying kernel transmit buffer size for the socket in bytes
	 *
	 * @see http://api.zeromq.org/2-1:zmq-setsockopt#toc13
	 */
	mixin SocketOption!(ulong, "sndbuf", zmq.ZMQ_SNDBUF);

	/**
	 * @name Receive buffer
	 *
	 * The underlying kernel receive buffer size for the socket in bytes
	 *
	 * @see http://api.zeromq.org/2-1:zmq-setsockopt#toc14
	 */
	mixin SocketOption!(ulong, "rcvbuf", zmq.ZMQ_RCVBUF);

	/**
	 * @name Linger period
	 *
	 * The amount of time a socket shall retain unsent messages after the socket closes, in milliseconds
	 *
	 * @see http://api.zeromq.org/2-1:zmq-setsockopt#toc15
	 */
	mixin SocketOption!(int, "linger", zmq.ZMQ_LINGER);

	/**
	 * @name Reconnection interval
	 *
	 * The period, in milliseconds, to wait between attempts to reconnect
	 * disconnected peers when using connection-oriented transports
	 *
	 * @see http://api.zeromq.org/2-1:zmq-setsockopt#toc16
	 */
	mixin SocketOption!(int, "reconnect_ivl", zmq.ZMQ_RECONNECT_IVL);

	/**
	 * @name Maximum reconnection interval
	 *
	 * The maximum period to wait between reconnection attempts
	 *
	 * @see http://api.zeromq.org/2-1:zmq-setsockopt#toc17
	 */
	mixin SocketOption!(int, "reconnect_ivl_max", zmq.ZMQ_RECONNECT_IVL_MAX);

	/**
	 * @name Backlog
	 *
	 * Maximum length of the queue of outstanding peer connections
	 * for connection-oriented transports
	 *
	 * @see http://api.zeromq.org/2-1:zmq-setsockopt#toc18
	 */
	mixin SocketOption!(int, "backlog",zmq.ZMQ_BACKLOG);

	/**
	 * @name Identity
	 *
	 * The socket's unique identity.
	 * This associates a socket with a particular infrastructure
	 * between program execution instances
	 * @see http://api.zeromq.org/2-1:zmq-setsockopt#toc6
	 * @{
	 */
	/// Setter
	void identity(string value) @property {
		if(zmq.zmq_setsockopt(this.socket, zmq.ZMQ_IDENTITY, cast(void*)value.toStringz(), value.length) == -1) {
			throw new ZMQException();
		}
	}
	/// Getter
	string identity() @property {
		size_t size=256;
		char[256] data;
		auto err = zmq.zmq_getsockopt(this.socket, zmq.ZMQ_IDENTITY, data.ptr, &size);
		if(err == -1) {
			throw new ZMQException();
		} else {
			return data[0..size].idup;
		}
	}
	// @}


	/**
	 * Checks for more parts of a message
	 *
	 * @return True if there is another message part queued
	 */
	bool more() @property {
		long ret;
		size_t size = ret.sizeof;
		if(zmq.zmq_getsockopt(this.socket, zmq.ZMQ_RCVMORE, &ret, &size) == -1) {
			throw new ZMQException();
		} else {
			return ret != 0;
		}
	}

	/**
	 * The type of the socket
	 *
	 * @return The socket's type
	 */
	Type type() @property @safe pure nothrow {
		return this._type;
	}

	/**
	 * Subscribe to a topic
	 *
	 * On a SUB socket, this creates a filter identifying messages to receive.
	 *
	 * If the \c topic is "", then the socket will subscribe to all messages.
	 *
	 * If the \c topic has a nonzero length, then the socket will subscribe to all
	 * messages beginning with the topic.
	 *
	 * @param topic The topic to subscribe to
	 * @see http://api.zeromq.org/2-1:zmq-setsockopt#toc7
	 */
	void subscribe(string topic) {
		if(zmq.zmq_setsockopt(this.socket, zmq.ZMQ_SUBSCRIBE, cast(void*)topic.toStringz(), topic.length) == -1) {
			throw new ZMQException();
		}
	}
	/**
	 * Unsubscribe to a topic
	 *
	 * On a SUB socket, this removes a filter identifying messages to receive
	 * @param topic The topic to unsubscribe from
	 * @see http://api.zeromq.org/2-1:zmq-setsockopt#toc8
	 */
	void unsubscribe(string topic) {
		if(zmq.zmq_setsockopt(this.socket, zmq.ZMQ_SUBSCRIBE, cast(void*)topic.toStringz(), topic.length) == -1) {
			throw new ZMQException();
		}
	}

	/**
	 * Raw socket access
	 *
	 * @return A pointer to the zmq socket data
	 */
	void* raw() @property @safe pure nothrow {
		return this.socket;
	}

}

/// D Range adaptor for sockets
class SocketStream {
private:
	Socket sock;
	string[] data;
	bool isWriteableSocket() {
		return([Type.REQ, Type.REP,
				Type.DEALER, Type.ROUTER,
				Type.PUB, Type.PUSH,
				Type.PAIR].canFind(sock.type));
	}
	bool isReadableSocket() {
		return([Type.REQ, Type.REP,
				Type.DEALER, Type.ROUTER,
				Type.SUB, Type.PULL,
				Type.PAIR].canFind(sock.type));
	}

public:
	/// Wrap all socket functions
	alias this = sock;

	/**
	 * Creates and initializes a socket
	 *
	 * @param sock The socket to wrap in a stream
	 */
	this(Socket sock) @safe nothrow {
		this.sock = sock;
	}
	/**
	 * Creates and initializes a socket
	 *
	 * @param context The 0MQ context to use for the socket's creation
	 * @param type The type of the socket
	 */
	this(Context context, Socket.Type type) @safe {
		this(new Socket(context, type));
	}

	// Input range interface

	/**
	 * Check to see if there is more data
	 *
	 * @return True if the socket exists
	 */
	bool empty() @property @safe pure nothrow {
		return sock is null || sock.raw is null;
	}

	/**
	 * Get the "current" data
	 *
	 * @return An array of strings received via 0MQ (a full "message")
	*/
	string[] front() @property {
		assert(!this.empty, "Attempting to read from unopened socket");
		assert(isReadableSocket(), "Socket is not readable");
		if(data.length==0) this.popFront();
		return this.data;
	}

	/**
	 * Get the next message from the socket
	 *
	 * Does not return anything, using the data requires using .front
	*/
	void popFront()
	{
		assert(!this.empty, "Attempting to read from unopened socket");
		assert(isReadableSocket(), "Socket is not readable");
		this.data = this.sock.recv_multipart();
	}

	// Output range interface

	/**
	 * Output a message to a stream
	 *
	 * @param strs A multipart message to send
	*/
	void put(string[] strs) {
		assert(!this.empty, "Attempting to read from unopened socket");
		assert(isWriteableSocket(), "Socket is not writeable");
		this.sock.send(strs);
	}
}

/**
 * ZMQ error class
 *
 * Automatically gets the latest ZMQ error
 */
class ZMQException : Exception {
public:
	/**
	 * Create and automatically initialize a ZMQException
	 */
	const int errno;
	this() {
		this.errno = zmq.zmq_errno();
		char* errmsg = zmq.zmq_strerror(this.errno);
		// Convert C string to D string
		super(format("%s", errmsg[0..strlen(errmsg)]));
	}
};

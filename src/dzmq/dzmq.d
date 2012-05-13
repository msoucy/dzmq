/** @file dzmq.d
@brief D ZeroMQ class wrappers
@authors Matthew Soucy <msoucy@csh.rit.edu>
@date May 9, 2012
@version 0.0.1
*/
///D ZeroMQ class wrappers
module dzmq;

private import ZeroMQ.zmq;

import std.string;
import std.conv;

/// ZeroMQ context manager
/**
Manages the context for all sockets within a thread
*/
class Context {
	private {
		void* context;
	}
	/**
	Constructor
	Create a 0MQ context to manage all sockets within a thread
	@param io_threads Number of threads to use for the context
	*/
	this(int io_threads=0) {
		this.context = zmq_init(io_threads);
	}
	~this() {
		zmq_term(this.context);
		context = null;
	}
	/**
	Get a raw pointer to the context
	@return Pointer to the block of data representing the context
	*/
	@property void* raw() {return context;}
}

/// ZeroMQ socket class
/**
Wraps a ZeroMQ socket and handles connections and data transfer
*/
class Socket {
	/// Socket types
	/**
	Types for a socket
	
	Defined to be the same as in ZMQ
	*/
	public immutable enum Type {
		/// Pair
		PAIR        = ZMQ_PAIR,
		/// Publisher
	    PUB         = ZMQ_PUB,
	    /// Subscriber
	    SUB         = ZMQ_SUB,
	    /// Request
	    REQ         = ZMQ_REQ,
	    /// Reply
	    REP         = ZMQ_REP,
	    /// Dealer
	    DEALER      = ZMQ_DEALER,
	    /// Router
	    ROUTER      = ZMQ_ROUTER,
	    /// Pulling
	    PULL        = ZMQ_PULL,
	    /// Pushing
	    PUSH        = ZMQ_PUSH,
	    /// Extended publisher
	    XPUB        = ZMQ_XPUB,
	    /// Extended subscriber
	    XSUB        = ZMQ_XSUB,
	}
	/**
	Message sending flags
	*/
	public immutable enum Flags {
		/// Send/receive nonblocking
		NOBLOCK = ZMQ_NOBLOCK,
		/// This is the first part of a sent message
		SNDMORE = ZMQ_SNDMORE,
	}
	
	private {
		void* socket;
		Type _type;
		
		/**
		Packs a string into a block of data
		@param destination Pointer to a valid block of data to write to
		@param data The string data to write
		*/
		void msg_pack(void* destination, string data) {
			size_t i=0;
			while(i < data.length){
				*cast(char*)(destination++) = data[i++];
			}
		}
		/**
		Unpacks a string from a zmq_msg_t
		@param msg The message to unpack
		@returns The stored string
		*/
		string msg_unpack(zmq_msg_t msg) {
			size_t i=zmq_msg_size(&msg);
			string ret=to!string(zmq_msg_data(&msg)[0..i]);
			return ret;
		}
	}
	
	/**
	@brief Creates and initializes a socket
	@param context The 0MQ context to use for the socket's creation
	@param type The type of the socket
	*/
	this(Context context, Type type) {
		socket = zmq_socket(context.raw, cast(int)type);
		this._type = type;
	}
	/**
	@brief Cleans up after a socket
	*/
	~this() {
		zmq_close(this.socket);
 	}
	
	/**
	@brief Bind a socket to an address
	@param addr The address to bind to
	*/
	void bind(string addr) {
		if(zmq_bind (this.socket, addr.toStringz()) != 0) {
			throw new ZMQError();
		}
	}
	/**
	@brief Connect a socket to an address
	@param endpoint The address to connect to
	*/
	void connect(string endpoint) {
		if(zmq_connect(this.socket, endpoint.toStringz()) != 0) {
			throw new ZMQError();
		}
	}
	
	/**
	@brief Send a message
	@param msg Data to send
	@param flags Send flags
	*/
	void send(string msg, int flags=0) {
		zmq_msg_t zmsg;
		zmq_msg_init_size(&zmsg, msg.length);
		scope(exit) zmq_msg_close (&zmsg);
		msg_pack(zmq_msg_data (&zmsg), msg);
		auto err = zmq_send(this.socket, &zmsg, flags);
		if(err != 0) {
			throw new ZMQError();
		}
	}
	/**
	Each string is sent as a separate part of the message 
	@brief Send a multipart message
	@param msg Data to send
	@param flags Send flags
	*/
	void send_multipart(string msg[], int flags=0) {
		for(size_t i=0; i+1 < msg.length; i++) {
			this.send(msg[i], flags|Flags.SNDMORE);
		}
		this.send(msg[$-1], flags);
	}
	
	/**
	@brief Receive a message
	@param flags Receive flags
	@returns A string storing the data received
	*/
	string recv(int flags=0) {
		zmq_msg_t zmsg;
		zmq_msg_init(&zmsg);
		scope(exit) zmq_msg_close(&zmsg);
		auto err = zmq_recv(this.socket, &zmsg, flags);
		string ret = msg_unpack(zmsg);
		if(err != 0) {
			throw new ZMQError();
		} else {
			return ret;
		}
	}
	/**
	Each string is received as a separate part of the message
	@brief Receive a message
	@param flags Receive flags
	@returns All data strings in the message
	*/
	string[] recv_multipart(int flags=0) {
		string[] parts = [];
		do {
			parts ~= this.recv(flags);
		} while(this.more);
		return parts;
	}
	
	/*
		Socket properties
	*/
	
	
	///Sets how many messages can build up in the socket's queue
	@property void hwm(ulong value) {
		if(zmq_setsockopt(this.socket, ZMQ_HWM, &value, ulong.sizeof)) {
			throw new ZMQError();
		}
	}
	/// High water mark
	@property ulong hwm() {
		ulong ret;
		size_t size;
		if(zmq_getsockopt(this.socket, ZMQ_HWM, &ret, &size)) {
			throw new ZMQError();
		} else {
			return ret;
		}
	}
	
	/// Set disk offload size
	@property void swap(long value) {
		if(zmq_setsockopt(this.socket, ZMQ_SWAP, &value, long.sizeof)) {
			throw new ZMQError();
		}
	}
	/// Get disk offload size
	@property long swap() {
		long ret;
		size_t size;
		if(zmq_getsockopt(this.socket, ZMQ_SWAP, &ret, &size)) {
			throw new ZMQError();
		} else {
			return ret;
		}
	}
	
	/// Set the I/O thread affinity
	@property void affinity(ulong value) {
		if(zmq_setsockopt(this.socket, ZMQ_AFFINITY, &value, ulong.sizeof)) {
			throw new ZMQError();
		}
	}
	/// Get the I/O thread affinity
	@property ulong affinity() {
		ulong ret;
		size_t size;
		if(zmq_getsockopt(this.socket, ZMQ_AFFINITY, &ret, &size)) {
			throw new ZMQError();
		} else {
			return ret;
		}
	}
	
	/// Set the multicast data rate
	void rate(long value) {
		if(zmq_setsockopt(this.socket, ZMQ_RATE, &value, long.sizeof)) {
			throw new ZMQError();
		}
	}
	/// Get the multicast data rate
	long rate() {
		long ret;
		size_t size;
		if(zmq_getsockopt(this.socket, ZMQ_RATE, &ret, &size)) {
			throw new ZMQError();
		} else {
			return ret;
		}
	}
	
	/// Set the multicast recovery interval
	@property void rec_ivl(long value) {
		if(zmq_setsockopt(this.socket, ZMQ_RECOVERY_IVL, &value, long.sizeof)) {
			throw new ZMQError();
		}
	}
	/// Get the multicast recovery interval
	@property long rec_ivl() {
		long ret;
		size_t size;
		if(zmq_getsockopt(this.socket, ZMQ_RECOVERY_IVL, &ret, &size)) {
			throw new ZMQError();
		} else {
			return ret;
		}
	}
	/// Set the multicast recovery interval in milliseconds
	@property void rec_ivl_msec(long value) {
		if(zmq_setsockopt(this.socket, ZMQ_RECOVERY_IVL_MSEC, &value, long.sizeof)) {
			throw new ZMQError();
		}
	}
	/// Get the multicast recovery interval in milliseconds
	@property long rec_ivl_msec() {
		long ret;
		size_t size;
		if(zmq_getsockopt(this.socket, ZMQ_RECOVERY_IVL_MSEC, &ret, &size)) {
			throw new ZMQError();
		} else {
			return ret;
		}
	}
	
	/// Set the multicast loopback
	@property void mcast(long value) {
		if(zmq_setsockopt(this.socket, ZMQ_MCAST_LOOP, &value, long.sizeof)) {
			throw new ZMQError();
		}
	}
	/// Get the multicast loopback
	@property long mcast() {
		long ret;
		size_t size;
		if(zmq_getsockopt(this.socket, ZMQ_MCAST_LOOP, &ret, &size)) {
			throw new ZMQError();
		} else {
			return ret;
		}
	}
	
	/// Set the kernel transmit buffer size
	@property void sndbuf(ulong value) {
		if(zmq_setsockopt(this.socket, ZMQ_SNDBUF, &value, ulong.sizeof)) {
			throw new ZMQError();
		}
	}
	/// Get the kernel transmit buffer size
	@property ulong sndbuf() {
		ulong ret;
		size_t size;
		if(zmq_getsockopt(this.socket, ZMQ_SNDBUF, &ret, &size)) {
			throw new ZMQError();
		} else {
			return ret;
		}
	}
	
	/// Set the kernel receive buffer size
	@property void rdvbuf(ulong value) {
		if(zmq_setsockopt(this.socket, ZMQ_RCVBUF, &value, ulong.sizeof)) {
			throw new ZMQError();
		}
	}
	/// Get the kernel receive buffer size
	@property ulong rcvbuf() {
		ulong ret;
		size_t size;
		if(zmq_getsockopt(this.socket, ZMQ_RCVBUF, &ret, &size)) {
			throw new ZMQError();
		} else {
			return ret;
		}
	}
	
	/// Set the socket shutdown linger period
	@property void linger(int value) {
		if(zmq_setsockopt(this.socket, ZMQ_LINGER, &value, int.sizeof)) {
			throw new ZMQError();
		}
	}
	/// Get the socket shutdown linger period
	@property int linger() {
		int ret;
		size_t size;
		if(zmq_getsockopt(this.socket, ZMQ_LINGER, &ret, &size)) {
			throw new ZMQError();
		} else {
			return ret;
		}
	}
	
	/// Set the reconnection interval
	@property void reconnect_ivl(int value) {
		if(zmq_setsockopt(this.socket, ZMQ_RECONNECT_IVL, &value, int.sizeof)) {
			throw new ZMQError();
		}
	}
	/// Get the reconnection interval
	@property int reconnect_ivl() {
		int ret;
		size_t size;
		if(zmq_getsockopt(this.socket, ZMQ_RECONNECT_IVL, &ret, &size)) {
			throw new ZMQError();
		} else {
			return ret;
		}
	}
	
	/// Set the maximum reconnection interval
	@property void reconnect_ivl_max(int value) {
		if(zmq_setsockopt(this.socket, ZMQ_RECONNECT_IVL_MAX, &value, int.sizeof)) {
			throw new ZMQError();
		}
	}
	/// Get the maximum reconnection interval
	@property int reconnect_ivl_max() {
		int ret;
		size_t size;
		if(zmq_getsockopt(this.socket, ZMQ_RECONNECT_IVL_MAX, &ret, &size)) {
			throw new ZMQError();
		} else {
			return ret;
		}
	}
	
	/// Set the maximum reconnection interval
	@property void backlog(int value) {
		if(zmq_setsockopt(this.socket, ZMQ_BACKLOG, &value, int.sizeof)) {
			throw new ZMQError();
		}
	}
	/// Get the maximum reconnection interval
	@property int backlog() {
		int ret;
		size_t size = ret.sizeof;
		if(zmq_getsockopt(this.socket, ZMQ_BACKLOG, &ret, &size)) {
			throw new ZMQError();
		} else {
			return ret;
		}
	}
	
	/// Set the socket identity
	@property void identity(string value) {
		if(zmq_setsockopt(this.socket, ZMQ_IDENTITY, cast(void*)value.toStringz(), value.length)) {
			throw new ZMQError();
		}
	}
	/// Get the socket identity
	@property string identity() {
		string ret;
		size_t size;
		auto err = zmq_getsockopt(this.socket, ZMQ_IDENTITY, &ret, &size);
		if(err) {
			throw new ZMQError();
		} else {
			return ret;
		}
	}
	
	// TODO: ZMQ_FD, ZMQ_events	
	
	/**
	@brief Checks for more parts of a message
	@returns True if there is another message part queued
	*/
	@property bool more() {
		long ret;
		size_t size = ret.sizeof;
		if(zmq_getsockopt(this.socket, ZMQ_RCVMORE, &ret, &size)) {
			throw new ZMQError();
		} else {
			return ret != 0;
		}
	}
	
	/**
	@brief The type of the socket 
	@returns The socket's type
	*/
	@property Type type() {
		return this._type;
	}
	
	/**
	@brief Subscribe to a topic
	@param topic The topic to subscribe to
	*/
	void subscribe(string topic) {
		if(zmq_setsockopt(this.socket, ZMQ_SUBSCRIBE, cast(void*)topic.toStringz(), topic.length)) {
			throw new ZMQError();
		}
	}
	/**
	@brief Unsubscribe to a topic
	@param topic The topic to unsubscribe from
	*/
	void unsubscribe(string topic) {
		if(zmq_setsockopt(this.socket, ZMQ_SUBSCRIBE, cast(void*)topic.toStringz(), topic.length)) {
			throw new ZMQError();
		}
	}
	
	/**
	@brief Raw socket access
	@returns A pointer to the zmq socket data
	*/
	@property ref void* raw() {
		return this.socket;
	}
	
}

/// ZMQ error class
/**
Automatically gets the latest ZMQ error
*/
class ZMQError : Error {
public:
	/**
	Create and automatically initialize a ZMQError
	*/
    this () {
    	char* errmsg = zmq_strerror(zmq_errno ());
    	string msg = "";
    	char* tmp = errmsg;
    	while(*tmp) {
    		msg ~= *(tmp++);
    	}
    	super( format("%s", msg/+, file, line +/));
	}
};

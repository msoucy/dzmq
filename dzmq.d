/** @file dzmq.d
@brief D ZeroMQ class wrappers
@authors Matthew Soucy <msoucy@csh.rit.edu>
@date May 9, 2012
@version 0.0.1
*/
///D ZeroMQ class wrappers
module dzmq;

/// @cond NoDoc
private import ZeroMQ.zmq;

import std.string : toStringz, format;
import std.algorithm : canFind;
/// @endcond

/** @brief ZeroMQ context manager
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

/** @brief ZeroMQ socket class
Wraps a ZeroMQ socket and handles connections and data transfer

@todo Add support for ZMQ_FD, to get the file descriptor (if valid in D)
@todo Add support for ZMQ_EVENTS - this requires ZMQ_POLLIN and ZMQ_POLLOUT to be wrapped
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
			string ret=cast(string)(zmq_msg_data(&msg)[0..i]).idup;
			return ret;
		}
		
		mixin template SocketOption(string NAME, int VALUE, TYPE) {
			/// Setter
			@property void SocketOption(TYPE value) {
				if(zmq_setsockopt(this.socket, VALUE, &value, TYPE.sizeof)) {
					throw new ZMQError();
				}
			}
			/// Getter
			@property TYPE SocketOption() {
				TYPE ret;
				size_t size = TYPE.sizeof;
				if(zmq_getsockopt(this.socket, VALUE, &ret, &size)) {
					throw new ZMQError();
				} else {
					return ret;
				}
			}
			mixin("alias SocketOption "~NAME~";");
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
	@brief Send a multipart message
	Each string is sent as a separate part of the message
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
	@brief Receive a message
	Each string is received as a separate part of the message
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
	
	/** @name High water mark
	
	The number of messages that can build up in the socket's queue
	@see http://api.zeromq.org/2-1:zmq-setsockopt#toc3
	*/
	mixin SocketOption!("hwm",ZMQ_HWM,ulong);
	
	/** @name Disk offload swap size
	
	The size (in bytes) of disk memory to store outstanding messages
	@see http://api.zeromq.org/2-1:zmq-setsockopt#toc4
	*/
	mixin SocketOption!("swap",ZMQ_SWAP,long);
	
	/** @name I/O thread affinity
	
	Determines which threads to use for socket I/O
	@see http://api.zeromq.org/2-1:zmq-setsockopt#toc5
	*/
	mixin SocketOption!("affinity",ZMQ_AFFINITY,ulong);
	
	/** @name Multicast data rate
	
	The maximum send or receive data rate for multicast transports
	@see http://api.zeromq.org/2-1:zmq-setsockopt#toc9
	*/
	mixin SocketOption!("rate",ZMQ_RATE,long);
	
	/** @name Multicast recovery interval
	
	The maximum time in seconds that a receiver can be absent from a multicast
	group before unrecoverable data loss will occur.
	
	@see http://api.zeromq.org/2-1:zmq-setsockopt#toc10
	@see http://api.zeromq.org/2-1:zmq-setsockopt#toc11
	*/
	mixin SocketOption!("rec_ivl",ZMQ_RECOVERY_IVL,long);
	mixin SocketOption!("rec_ivl_msec",ZMQ_RECOVERY_IVL_MSEC,long);
	
	/** @name Multicast loopback
	
	Enables or disables the ability to receive transports from itself via loopback
	@see http://api.zeromq.org/2-1:zmq-setsockopt#toc12
	*/
	mixin SocketOption!("mcast",ZMQ_MCAST_LOOP,long);
	
	/** @name Send buffer
	
	The underlying kernel transmit buffer size for the socket in bytes
	@see http://api.zeromq.org/2-1:zmq-setsockopt#toc13
	*/
	mixin SocketOption!("sndbuf",ZMQ_SNDBUF,ulong);
	
	/** @name Receive buffer
	
	The underlying kernel receive buffer size for the socket in bytes
	@see http://api.zeromq.org/2-1:zmq-setsockopt#toc14
	*/
	mixin SocketOption!("rcvbuf",ZMQ_RCVBUF,ulong);
	
	/** @name Linger period
	
	The amount of time a socket shall retain unsent messages after the socket closes, in milliseconds
	@see http://api.zeromq.org/2-1:zmq-setsockopt#toc15
	*/
	mixin SocketOption!("linger",ZMQ_LINGER,int);
	
	/** @name Reconnection interval
	
	The period, in milliseconds, to wait between attempts to reconnect
	disconnected peers when using connection-oriented transports
	@see http://api.zeromq.org/2-1:zmq-setsockopt#toc16
	*/
	mixin SocketOption!("reconnect_ivl",ZMQ_RECONNECT_IVL,int);
	
	/** @name Maximum reconnection interval
	
	The maximum period to wait between reconnection attempts
	@see http://api.zeromq.org/2-1:zmq-setsockopt#toc17
	*/
	mixin SocketOption!("reconnect_ivl_max",ZMQ_RECONNECT_IVL_MAX,int);
	
	/** @name Backlog
	
	Maximum length of the queue of outstanding peer connections
	for connection-oriented transports
	@see http://api.zeromq.org/2-1:zmq-setsockopt#toc18
	*/
	mixin SocketOption!("backlog",ZMQ_BACKLOG,int);
	
	/** @name Identity
	
	The socket's unique identity.
	This associates a socket with a particular infrastructure
	between program execution instances
	@see http://api.zeromq.org/2-1:zmq-setsockopt#toc6
	@{
	*/
	/// Setter
	@property void identity(string value) {
		if(zmq_setsockopt(this.socket, ZMQ_IDENTITY, cast(void*)value.toStringz(), value.length)) {
			throw new ZMQError();
		}
	}
	/// Getter
	@property string identity() {
		string ret;
		size_t size=256;
		char[256] data;
		auto err = zmq_getsockopt(this.socket, ZMQ_IDENTITY, data.ptr, &size);
		if(err) {
			throw new ZMQError();
		} else {
			ret = data[0..size].idup;
			return ret;
		}
	}
	// @}
	
	
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
	
	On a SUB socket, this creates a filter identifying messages to receive.
	
	If the \c topic is "", then the socket will subscribe to all messages.
	
	If the \c topic has a nonzero length, then the socket will subscribe to all
	messages beginning with the topic.
	
	@param topic The topic to subscribe to
	@see http://api.zeromq.org/2-1:zmq-setsockopt#toc7
	*/
	void subscribe(string topic) {
		if(zmq_setsockopt(this.socket, ZMQ_SUBSCRIBE, cast(void*)topic.toStringz(), topic.length)) {
			throw new ZMQError();
		}
	}
	/**
	@brief Unsubscribe to a topic
	
	On a SUB socket, this removes a filter identifying messages to receive
	@param topic The topic to unsubscribe from
	@see http://api.zeromq.org/2-1:zmq-setsockopt#toc8
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

/**
@brief D Range adaptor for sockets
*/
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
	alias sock this;
	
	/**
	@brief Creates and initializes a socket
	@param sock The socket to wrap in a stream
	*/
	this(Socket sock) {
		this.sock = sock;
	}
	/**
	@brief Creates and initializes a socket
	@param context The 0MQ context to use for the socket's creation
	@param type The type of the socket
	*/
	this(Context context, Socket.Type type) {
		this(new Socket(context, type));
	}
	
	// Input range interface
	
	/**
	@brief Check to see if there is more data
	@return True if the socket exists
	*/
	@property bool empty() {
		return sock is null || sock.raw is null;
	}
	
	/**
	@brief Get the "current" data
	@return An array of strings received via 0MQ (a full "message")
	*/
	@property string[] front()
	{
		assert(!this.empty, "Attempting to read from unopened socket");
		assert(isReadableSocket(), "Socket is not readable");
		if(data.length==0) this.popFront();
		return this.data;
	}

	/**
	@brief Get the next message from the socket
	
	Does not return anything, using the data requires using .front
	*/
	void popFront()
	{
		assert(!this.empty, "Attempting to read from unopened socket");
		assert(isReadableSocket(), "Socket is not readable");
		this.data = this.sock.recv_multipart();
	}
	
	// Output range interface
	
	/**
	@brief Output a message to a stream
	@param strs A multipart message to send
	*/
	void put(string[] strs) {
		assert(!this.empty, "Attempting to read from unopened socket");
		assert(isWriteableSocket(), "Socket is not writeable");
		this.sock.send_multipart(strs);
	}
}

/** @brief ZMQ error class
Automatically gets the latest ZMQ error
*/
class ZMQError : Error {
public:
	/**
	Create and automatically initialize a ZMQError
	*/
    this () {
    	char* errmsg = zmq_strerror(zmq_errno ());
    	// Convert C string to D string
    	string msg = "";
    	char* tmp = errmsg;
    	while(*tmp) {
    		msg ~= *(tmp++);
    	}
    	super(format("%s", msg));
	}
};

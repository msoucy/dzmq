module dzmq;

private import zmq;

import std.string;
import std.conv;


void fillmessage(void* destination, string data, int size) {
	int i=0;
	while(i<size){
		*cast(char*)(destination++) = data[i++];
	}
}

class Context {
	private {
		void* context;
	}
	this(int io_threads=0) {
		this.context = zmq_init(io_threads);
	}
	~this() {
		zmq_term(this.context);
		context = null;
	}
	@property void* raw() {return context;}
}

class Socket {
	public immutable enum Type {
		PAIR        = ZMQ_PAIR,
	    PUB         = ZMQ_PUB,
	    SUB         = ZMQ_SUB,
	    REQ         = ZMQ_REQ,
	    REP         = ZMQ_REP,
	    DEALER      = ZMQ_DEALER,
	    ROUTER      = ZMQ_ROUTER,
	    PULL        = ZMQ_PULL,
	    PUSH        = ZMQ_PUSH,
	    XPUB        = ZMQ_XPUB,
	    XSUB        = ZMQ_XSUB,
	}
	public immutable enum Flags {
		NOBLOCK = ZMQ_NOBLOCK,
		SNDMORE = ZMQ_SNDMORE,
	}
	
	private {
		void* socket;
		Type _type;
		void msg_pack(void* destination, string data) {
			size_t i=0;
			while(i < data.length){
				*cast(char*)(destination++) = data[i++];
			}
		}
		string msg_unpack(zmq_msg_t msg) {
			size_t i=zmq_msg_size(&msg);
			string ret=to!string(zmq_msg_data(&msg)[0..i]);
			return ret;
		}
	}
	
	this(Context context, Type type) {
		socket = zmq_socket(context.raw, cast(int)type);
		this._type = type;
	}
	~this() {
		zmq_close(this.socket);
 	}
	
	void bind(string addr) {
		if(zmq_bind (this.socket, addr.toStringz) != 0) {
			throw new ZMQError();
		}
	}
	void connect(string endpoint) {
		if(zmq_connect(this.socket, endpoint.toStringz) != 0) {
			throw new ZMQError();
		}
	}
	
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
	void send_multipart(string msg[], int flags=0) {
		for(size_t i=0; i+1 < msg.length; i++) {
			this.send(msg[i], flags|Flags.SNDMORE);
		}
		this.send(msg[$-1], flags);
	}
	
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
	string[] recv_multipart(int flags=0) {
		string[] parts = [];
		do {
			parts ~= this.recv(flags);
		} while(this.more);
		return parts;
	}
	
	@property {
		// High water mark
		void hwm(ulong value) {
			if(zmq_setsockopt(this.socket, ZMQ_HWM, &value, ulong.sizeof)) {
				throw new ZMQError();
			}
		}
		ulong hwm() {
			ulong ret;
			size_t size;
			if(zmq_getsockopt(this.socket, ZMQ_HWM, &ret, &size)) {
				throw new ZMQError();
			} else {
				return ret;
			}
		}
		
		// Swap: Set disk offload size
		void swap(long value) {
			if(zmq_setsockopt(this.socket, ZMQ_SWAP, &value, long.sizeof)) {
				throw new ZMQError();
			}
		}
		long swap() {
			long ret;
			size_t size;
			if(zmq_getsockopt(this.socket, ZMQ_SWAP, &ret, &size)) {
				throw new ZMQError();
			} else {
				return ret;
			}
		}
		
		// Affinity: I/O thread affinity
		void affinity(ulong value) {
			if(zmq_setsockopt(this.socket, ZMQ_AFFINITY, &value, ulong.sizeof)) {
				throw new ZMQError();
			}
		}
		ulong affinity() {
			ulong ret;
			size_t size;
			if(zmq_getsockopt(this.socket, ZMQ_AFFINITY, &ret, &size)) {
				throw new ZMQError();
			} else {
				return ret;
			}
		}
		
		// Rate: multicast data rate
		void rate(long value) {
			if(zmq_setsockopt(this.socket, ZMQ_RATE, &value, long.sizeof)) {
				throw new ZMQError();
			}
		}
		long rate() {
			long ret;
			size_t size;
			if(zmq_getsockopt(this.socket, ZMQ_RATE, &ret, &size)) {
				throw new ZMQError();
			} else {
				return ret;
			}
		}
		
		// Multicast recovery interval
		void rec_ivl(long value) {
			if(zmq_setsockopt(this.socket, ZMQ_RECOVERY_IVL, &value, long.sizeof)) {
				throw new ZMQError();
			}
		}
		long rec_ivl() {
			long ret;
			size_t size;
			if(zmq_getsockopt(this.socket, ZMQ_RECOVERY_IVL, &ret, &size)) {
				throw new ZMQError();
			} else {
				return ret;
			}
		}
		void rec_ivl_msec(long value) {
			if(zmq_setsockopt(this.socket, ZMQ_RECOVERY_IVL_MSEC, &value, long.sizeof)) {
				throw new ZMQError();
			}
		}
		long rec_ivl_msec() {
			long ret;
			size_t size;
			if(zmq_getsockopt(this.socket, ZMQ_RECOVERY_IVL_MSEC, &ret, &size)) {
				throw new ZMQError();
			} else {
				return ret;
			}
		}
		
		// Multicast loopback
		void mcast(long value) {
			if(zmq_setsockopt(this.socket, ZMQ_MCAST_LOOP, &value, long.sizeof)) {
				throw new ZMQError();
			}
		}
		long mcast() {
			long ret;
			size_t size;
			if(zmq_getsockopt(this.socket, ZMQ_MCAST_LOOP, &ret, &size)) {
				throw new ZMQError();
			} else {
				return ret;
			}
		}
		
		// Kernel transmit buffer size
		void sndbuf(ulong value) {
			if(zmq_setsockopt(this.socket, ZMQ_SNDBUF, &value, ulong.sizeof)) {
				throw new ZMQError();
			}
		}
		ulong sndbuf() {
			ulong ret;
			size_t size;
			if(zmq_getsockopt(this.socket, ZMQ_SNDBUF, &ret, &size)) {
				throw new ZMQError();
			} else {
				return ret;
			}
		}
		
		// Kernel receive buffer size
		void rdvbuf(ulong value) {
			if(zmq_setsockopt(this.socket, ZMQ_RCVBUF, &value, ulong.sizeof)) {
				throw new ZMQError();
			}
		}
		ulong rcvbuf() {
			ulong ret;
			size_t size;
			if(zmq_getsockopt(this.socket, ZMQ_RCVBUF, &ret, &size)) {
				throw new ZMQError();
			} else {
				return ret;
			}
		}
		
		// Socket shutdown linger period
		void linger(int value) {
			if(zmq_setsockopt(this.socket, ZMQ_LINGER, &value, int.sizeof)) {
				throw new ZMQError();
			}
		}
		int linger() {
			int ret;
			size_t size;
			if(zmq_getsockopt(this.socket, ZMQ_LINGER, &ret, &size)) {
				throw new ZMQError();
			} else {
				return ret;
			}
		}
		
		// Reconnection interval
		void reconnect_ivl(int value) {
			if(zmq_setsockopt(this.socket, ZMQ_RECONNECT_IVL, &value, int.sizeof)) {
				throw new ZMQError();
			}
		}
		int reconnect_ivl() {
			int ret;
			size_t size;
			if(zmq_getsockopt(this.socket, ZMQ_RECONNECT_IVL, &ret, &size)) {
				throw new ZMQError();
			} else {
				return ret;
			}
		}
		
		// Maximum reconnection interval
		void reconnect_ivl_max(int value) {
			if(zmq_setsockopt(this.socket, ZMQ_RECONNECT_IVL_MAX, &value, int.sizeof)) {
				throw new ZMQError();
			}
		}
		int reconnect_ivl_max() {
			int ret;
			size_t size;
			if(zmq_getsockopt(this.socket, ZMQ_RECONNECT_IVL_MAX, &ret, &size)) {
				throw new ZMQError();
			} else {
				return ret;
			}
		}
		
		// Maximum reconnection interval
		void backlog(int value) {
			if(zmq_setsockopt(this.socket, ZMQ_BACKLOG, &value, int.sizeof)) {
				throw new ZMQError();
			}
		}
		int backlog() {
			int ret;
			size_t size = ret.sizeof;
			if(zmq_getsockopt(this.socket, ZMQ_BACKLOG, &ret, &size)) {
				throw new ZMQError();
			} else {
				return ret;
			}
		}
		
		// Socket identity
		void identity(string value) {
			if(zmq_setsockopt(this.socket, ZMQ_IDENTITY, cast(void*)value.toStringz, value.length)) {
				throw new ZMQError();
			}
		}
		string identity() {
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
		
		bool more() {
			long ret;
			size_t size = ret.sizeof;
			if(zmq_getsockopt(this.socket, ZMQ_RCVMORE, &ret, &size)) {
				throw new ZMQError();
			} else {
				return ret != 0;
			}
		}
		
		Type type() {
			return this._type;
		}
		
	}
	
	// Subscribe and unsubscribe
	void subscribe(string value) {
		if(zmq_setsockopt(this.socket, ZMQ_SUBSCRIBE, cast(void*)value.toStringz, value.length)) {
			throw new ZMQError();
		}
	}
	void unsubscribe(string value) {
		if(zmq_setsockopt(this.socket, ZMQ_SUBSCRIBE, cast(void*)value.toStringz, value.length)) {
			throw new ZMQError();
		}
	}
	
	// Raw access
	@property ref void* raw() {
		return this.socket;
	}
	
}

class ZMQError : Error {
public:
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

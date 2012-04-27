module dzmq;

private import zmq;

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
	}
	alias context this;
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
	}
	this(Context context, Type type) {
		socket = zmq_socket(context, cast(int)type);
	}
}

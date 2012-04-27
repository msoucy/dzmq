module dzmq;

private import zmq;

import std.string;


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
		void fillmessage(void* destination, string data) {
			int i=0;
			while(i < data.length){
				*cast(char*)(destination++) = data[i++];
			}
		}
	}
	
	this(Context context, Type type) {
		socket = zmq_socket(context.raw, cast(int)type);
	}
	~this() {
		zmq_close(this.socket);
 	}
	
	int bind(string addr) {
		if(zmq_bind (this.socket, addr.toStringz) != 0) {
			throw new ZMQError();
		} else {
			return 0;
		}
	}
	int connect(string endpoint) {
		if(zmq_connect(this.socket, endpoint.toStringz) != 0) {
			throw new ZMQError();
		} else {
			return 0;
		}
	}
	int send(string msg, int flags) {
		zmq_msg_t zmsg;
		zmq_msg_init_size(&zmsg, msg.length);
		fillmessage(zmq_msg_data (&zmsg), msg);
		auto err = zmq_send(this.socket, &zmsg, flags);
		zmq_msg_close (&zmsg);
		if(err != 0) {
			throw new ZMQError();
		} else {
			return 0;
		}
	}
	int recv(int flags) {
		zmq_msg_t zmsg;
		zmq_msg_init(&zmsg);
		auto err = zmq_recv(this.socket, &zmsg, flags);
		zmq_msg_close(&zmsg);
		if(err != 0) {
			throw new ZMQError();
		} else {
			return 0;
		}
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
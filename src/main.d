
import std.stdio;

import zmq;
import dzmq;

void cmain() {
	Context context = new Context(1);
	
	// Socket to talk to server
	printf("Connecting to hello world server…\n");
	void *requester = zmq_socket(context.raw, ZMQ_REQ);
	zmq_connect (requester, "tcp://localhost:5555");
	
	int request_nbr;
	for (request_nbr = 0; request_nbr != 10; request_nbr++) {
		zmq_msg_t request;
		zmq_msg_init_size (&request, 5);
		fillmessage (zmq_msg_data (&request), "Hello", 5);
		printf ("Sending Hello %d…\n", request_nbr);
		zmq_send (requester, &request, 0);
		zmq_msg_close (&request);
		
		zmq_msg_t reply;
		zmq_msg_init (&reply);
		zmq_recv (requester, &reply, 0);
		printf ("Received World %d\n", request_nbr);
		zmq_msg_close (&reply);
	}
	zmq_close (requester);
}

void smain()
{
	Context context = new Context(1);
	
	// Socket to talk to clients
	void *responder = zmq_socket (context.raw, ZMQ_REP);
	zmq_bind (responder, "tcp://*:5555");
	
	while (1) {
		// Wait for next request from client
		zmq_msg_t request;
		zmq_msg_init (&request);
		zmq_recv (responder, &request, 0);
		printf ("Received Hello\n");
		zmq_msg_close (&request);
		
		// Do some 'work'
		//sleep (1);
		foreach(i;0..100000){}
		
		// Send reply back to client
		zmq_msg_t reply;
		zmq_msg_init_size (&reply, 5);
		fillmessage(zmq_msg_data (&reply), "World", 5);
		zmq_send (responder, &reply, 0);
		zmq_msg_close (&reply);
	}
	// We never get here but if we did, this would be how we end
	zmq_close (responder);
}

void main(string[] argv) {
	if(argv.length != 2) {
		stderr.writeln("Error: Invalid arguments");
		return;
	}
	if(argv[1] == "server") smain();
	else cmain();
}